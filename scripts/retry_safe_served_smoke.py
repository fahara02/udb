#!/usr/bin/env python3
"""Served-path proof for replay-safe mutation retry metadata.

This smoke does two things together:

1. verifies the generated Python SDK gate treats DataBroker.Upsert as
   replay-safe only when the request carries a non-empty idempotency key; and
2. in live mode, replays the same keyed Upsert and Delete against a broker and
   requires the served responses to return `was_duplicate=true`.

It does not fake a transient broker failure. That remains the SDK unit-test
surface. The live proof here ties the retry metadata to the broker's durable
dedup behavior, which is the safety precondition for mutation auto-retry.
Live mode requires --require-all-proofs so workflow evidence cannot be narrowed
to a partial proof by dropping one replay side.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import re
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY_SDK = ROOT / "sdk" / "python"
PY_GEN = PY_SDK / "gen"
for path in (PY_SDK, PY_GEN):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import grpc  # type: ignore  # noqa: E402
from google.protobuf.json_format import ParseDict, ParseError  # noqa: E402
from udb.entity.v1.mutation_pb2 import MutationResponse  # noqa: E402
from udb.entity.v1.relational_pb2 import DeleteRequest, UpsertRequest  # noqa: E402
from udb.services.v1 import data_broker_pb2  # noqa: E402
from udb.services.v1.data_broker_pb2_grpc import DataBrokerStub  # noqa: E402
from udb_client.generated_client import (  # noqa: E402
    RPC_REPLAY_SAFE,
    RetryPolicy,
    _is_replay_safe,
    _request_has_idempotency_key,
)


UPSERT_METHOD = "/udb.services.v1.DataBroker/Upsert"
DELETE_METHOD = "/udb.services.v1.DataBroker/Delete"
DOCUMENT_UPSERT_METHOD = "/udb.services.v1.DataBroker/DocumentUpsert"
MAX_LIVE_TIMEOUT_SECONDS = 120.0
TIMEOUT_DECIMAL_PATTERN = re.compile(r"^(?:[1-9]\d*(?:\.\d+)?|0\.\d*[1-9]\d*)$")
MAX_PROOF_INPUT_BYTES = 1_048_576
MAX_LIVE_METADATA_COUNT = 32
MAX_LIVE_METADATA_VALUE_BYTES = 8_192
GRPC_METADATA_NAME_CHARS = frozenset("0123456789abcdefghijklmnopqrstuvwxyz_.-")
MANIFEST_CHECKSUM_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
MUTATION_ID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
DEFAULT_MUTATION_ID = "11111111-1111-4111-8111-111111111111"
SUMMARY_WRITE_RECEIPT_JSON = (
    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
    '"written_at_unix_ms":1700000000000}'
)
SUMMARY_WRITE_RECEIPT_DICT = json.loads(SUMMARY_WRITE_RECEIPT_JSON)


def _mutation_response(**kwargs) -> MutationResponse:
    kwargs.setdefault("mutation_id", DEFAULT_MUTATION_ID)
    return MutationResponse(**kwargs)


def _clone_request(request: UpsertRequest) -> UpsertRequest:
    clone = request.__class__()
    clone.ParseFromString(request.SerializeToString())
    return clone


def _parse_header(value: str) -> tuple[str, str]:
    if ":" not in value:
        raise ValueError(f"header must be 'Name: Value', got {value!r}")
    raw_name, raw = value.split(":", 1)
    name = raw_name.strip()
    raw_value = raw[1:] if raw.startswith(" ") else raw
    val = raw_value.strip()
    if not name or not val:
        raise ValueError(f"header must be 'Name: Value', got {value!r}")
    if raw_name != name:
        raise ValueError("gRPC metadata header name must not include surrounding whitespace")
    if raw_value != val:
        raise ValueError("gRPC metadata header value must not include surrounding whitespace")
    normalized = name.lower()
    if name != normalized:
        raise ValueError("gRPC metadata header name must contain only lowercase letters, digits, '.', '_', or '-'")
    if any(ch not in GRPC_METADATA_NAME_CHARS for ch in normalized):
        raise ValueError("gRPC metadata header name must contain only lowercase letters, digits, '.', '_', or '-'")
    if normalized.startswith("grpc-"):
        raise ValueError("gRPC metadata header name must not start with grpc-")
    if normalized.endswith("-bin"):
        raise ValueError("gRPC binary metadata headers are not supported by --header")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in val):
        raise ValueError("gRPC metadata header value must not contain control characters")
    if len(val.encode("utf-8")) > MAX_LIVE_METADATA_VALUE_BYTES:
        raise ValueError(f"gRPC metadata header value must be <= {MAX_LIVE_METADATA_VALUE_BYTES} bytes")
    return (normalized, val)


def _parse_headers(values: list[str]) -> tuple[tuple[str, str], ...]:
    parsed: list[tuple[str, str]] = []
    seen: set[str] = set()
    if len(values) > MAX_LIVE_METADATA_COUNT:
        raise ValueError(f"gRPC metadata headers must be <= {MAX_LIVE_METADATA_COUNT} entries")
    for value in values:
        name, header_value = _parse_header(value)
        if name in seen:
            raise ValueError(f"duplicate gRPC metadata header {name!r}")
        seen.add(name)
        parsed.append((name, header_value))
    return tuple(parsed)


def _contains_control_character(value: str) -> bool:
    return any(ord(ch) < 32 or ord(ch) == 127 for ch in value)


def validate_grpc_target(target: str) -> str:
    if target != target.strip():
        raise ValueError("gRPC target must not include surrounding whitespace")
    if not target:
        raise ValueError("gRPC target must be non-empty")
    if _contains_control_character(target):
        raise ValueError("gRPC target must not include control characters")
    if any(ch.isspace() for ch in target):
        raise ValueError("gRPC target must not include whitespace")
    if "://" in target or any(ch in target for ch in "/?#"):
        raise ValueError("gRPC target must be a host:port authority, not a URL or path")
    if target.startswith("["):
        close = target.find("]")
        if close <= 1 or close + 1 >= len(target) or target[close + 1] != ":":
            raise ValueError("gRPC target must be host:port or [ipv6]:port")
        host = target[1:close]
        port = target[close + 2 :]
    else:
        if ":" not in target:
            raise ValueError("gRPC target must include a port")
        host, port = target.rsplit(":", 1)
    if not host:
        raise ValueError("gRPC target host must be non-empty")
    if not port.isdigit() or not (1 <= int(port) <= 65535):
        raise ValueError("gRPC target port must be an integer from 1 to 65535")
    return target


def normalize_timeout_seconds(timeout: float | str) -> float:
    if isinstance(timeout, str):
        raw = timeout
        stripped = raw.strip()
        if raw != stripped:
            raise ValueError("timeout must not include surrounding whitespace")
        if not TIMEOUT_DECIMAL_PATTERN.fullmatch(stripped):
            raise ValueError("timeout must be a positive decimal number of seconds")
        parsed = float(stripped)
    else:
        parsed = float(timeout)
    if not math.isfinite(parsed):
        raise ValueError("timeout must be a finite number of seconds")
    if parsed <= 0:
        raise ValueError("timeout must be greater than 0 seconds")
    if parsed > MAX_LIVE_TIMEOUT_SECONDS:
        raise ValueError("timeout must be <= 120 seconds")
    return parsed


def validate_timeout_seconds(timeout: float | str) -> float:
    return normalize_timeout_seconds(timeout)


def validate_runtime_metadata(label: str, metadata: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(metadata, tuple):
        raise ValueError(f"{label} runtime metadata must be a parsed gRPC metadata tuple")
    if len(metadata) > MAX_LIVE_METADATA_COUNT:
        raise ValueError(f"{label} runtime gRPC metadata headers must be <= {MAX_LIVE_METADATA_COUNT} entries")
    parsed: list[tuple[str, str]] = []
    seen: set[str] = set()
    for index, item in enumerate(metadata):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(f"{label} runtime metadata entry {index} must be a (name, value) tuple")
        name, value = item
        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError(f"{label} runtime metadata entry {index} name and value must be strings")
        try:
            parsed_name, parsed_value = _parse_header(f"{name}: {value}")
        except ValueError as error:
            raise ValueError(f"{label} runtime metadata entry {index}: {error}") from error
        if parsed_name in seen:
            raise ValueError(f"{label} runtime duplicate gRPC metadata header {parsed_name!r}")
        seen.add(parsed_name)
        parsed.append((parsed_name, parsed_value))
    return tuple(parsed)


def validate_runtime_timeout_seconds(label: str, timeout: object) -> float:
    try:
        return validate_timeout_seconds(timeout)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} runtime timeout is invalid: {error}") from error


def validate_runtime_transport_inputs(label: str, metadata: object, timeout: object) -> tuple[tuple[tuple[str, str], ...], float]:
    return validate_runtime_metadata(label, metadata), validate_runtime_timeout_seconds(label, timeout)


def validate_complete_proof_mode(enabled: bool) -> None:
    if not enabled:
        raise ValueError("--require-all-proofs is required for retry-safe live served proof")


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict:
    out: dict[str, object] = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"proof JSON must not contain duplicate key {key!r}")
        out[key] = value
    return out


def _reject_non_finite_json_constant(constant: str) -> None:
    raise ValueError(f"proof JSON must not contain non-standard constant {constant}")


def _normalize_upsert_dict(data: dict) -> dict:
    normalized = dict(data)
    record_json_forms = [key for key in ("record_json", "record_json_object", "record_json_text") if key in normalized]
    if len(record_json_forms) > 1:
        raise ValueError(
            "Upsert proof input must use only one of record_json, record_json_object, or record_json_text"
        )
    if "record_json_object" in normalized:
        record_json_object = normalized.pop("record_json_object")
        if not isinstance(record_json_object, dict):
            raise ValueError("record_json_object must be a JSON object")
        raw = json.dumps(record_json_object, separators=(",", ":"), sort_keys=True).encode("utf-8")
        normalized["record_json"] = base64.b64encode(raw).decode("ascii")
    if "record_json_text" in normalized:
        record_json_text = normalized.pop("record_json_text")
        if not isinstance(record_json_text, str):
            raise ValueError("record_json_text must be a string")
        raw = record_json_text.encode("utf-8")
        normalized["record_json"] = base64.b64encode(raw).decode("ascii")
    return normalized


def _read_proof_text(path: Path, label: str) -> str:
    if not path.is_file():
        raise ValueError(f"{label} proof file must exist and be a regular file: {path}")
    try:
        size = path.stat().st_size
    except OSError as error:
        raise ValueError(f"{label} proof file is not readable: {path}: {error}") from error
    if size > MAX_PROOF_INPUT_BYTES:
        raise ValueError(f"{label} proof file must be <= {MAX_PROOF_INPUT_BYTES} bytes: {path}")
    try:
        return path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"{label} proof file is not readable: {path}: {error}") from error


def load_upsert(path: Path, label: str = "Upsert") -> UpsertRequest:
    try:
        data = json.loads(
            _read_proof_text(path, label),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except ValueError as error:
        raise ValueError(f"{path}: {error}") from error
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a JSON object")
    request = UpsertRequest()
    try:
        ParseDict(_normalize_upsert_dict(data), request)
    except ParseError as error:
        raise ValueError(f"{path}: {label} proof JSON does not match UpsertRequest: {error}") from error
    return request


def load_delete(path: Path, label: str = "Delete") -> DeleteRequest:
    try:
        data = json.loads(
            _read_proof_text(path, label),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except ValueError as error:
        raise ValueError(f"{path}: {error}") from error
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a JSON object")
    request = DeleteRequest()
    try:
        ParseDict(data, request)
    except ParseError as error:
        raise ValueError(f"{path}: {label} proof JSON does not match DeleteRequest: {error}") from error
    return request


def validate_replay_request(label: str, request) -> None:
    validate_replay_idempotency_key(f"{label} proof", request.idempotency_key)
    validate_replay_scope_token(f"{label} proof context.tenant_id", request.context.tenant_id)
    validate_replay_scope_token(f"{label} proof context.project_id", request.context.project_id)
    validate_message_type_token(f"{label} proof message_type", request.message_type)
    if isinstance(request, UpsertRequest):
        validate_upsert_payload(request)
    if isinstance(request, DeleteRequest):
        validate_delete_filter(request)


def validate_runtime_upsert_request(label: str, request: object) -> UpsertRequest:
    if not isinstance(request, UpsertRequest):
        raise ValueError(f"{label} runtime request must be an UpsertRequest")
    validate_replay_request(label, request)
    return request


def validate_runtime_delete_request(label: str, request: object) -> DeleteRequest:
    if not isinstance(request, DeleteRequest):
        raise ValueError(f"{label} runtime request must be a DeleteRequest")
    validate_replay_request(label, request)
    return request


def assert_databroker_method_request(method: str, request: object, expected_method: str) -> None:
    service = data_broker_pb2.DESCRIPTOR.services_by_name.get("DataBroker")
    if service is None:
        raise AssertionError("DataBroker generated service descriptor was not found")
    prefix = "/udb.services.v1.DataBroker/"
    if not method.startswith(prefix):
        raise AssertionError(f"retry-safe method constant {method!r} must target {prefix}")
    method_name = method[len(prefix) :]
    if method_name != expected_method:
        raise AssertionError(
            f"retry-safe method constant {method!r} names {method_name!r}, expected {expected_method!r}"
        )
    method_descriptor = service.methods_by_name.get(method_name)
    if method_descriptor is None:
        raise AssertionError(f"DataBroker generated descriptor has no method {method_name}")
    request_descriptor = getattr(request, "DESCRIPTOR", None)
    request_name = getattr(request_descriptor, "full_name", "")
    expected_request_name = method_descriptor.input_type.full_name
    if request_name != expected_request_name:
        raise AssertionError(
            f"{method_name} request message {request_name or '<unknown>'} does not match RPC input "
            f"{expected_request_name}"
        )


def validate_runtime_stub_method(label: str, stub: object, method_name: str):
    method = getattr(stub, method_name, None)
    if not callable(method):
        raise ValueError(f"{label} runtime stub must expose callable {method_name}")
    return method


def validate_runtime_mutation_response(label: str, response: object) -> MutationResponse:
    if not isinstance(response, MutationResponse):
        raise AssertionError(f"{label} runtime response must be a MutationResponse")
    return response


def call_runtime_mutation(label: str, method, request, metadata, timeout: float) -> MutationResponse:
    try:
        response = method(_clone_request(request), metadata=metadata, timeout=timeout)
    except grpc.RpcError as error:
        raise AssertionError(f"{label} runtime call raised unexpected gRPC error: {error}") from error
    except Exception as error:
        raise AssertionError(f"{label} runtime call raised error: {error}") from error
    return validate_runtime_mutation_response(label, response)


def validate_replay_idempotency_key(label: str, value: object) -> None:
    text = str(value or "")
    stripped = text.strip()
    if not stripped:
        raise ValueError(f"{label} requires a non-empty idempotency_key")
    if text != stripped:
        raise ValueError(f"{label} idempotency_key must not include surrounding whitespace")
    if any(char.isspace() for char in stripped):
        raise ValueError(f"{label} idempotency_key must not include whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in stripped):
        raise ValueError(f"{label} idempotency_key must not contain control characters")


def validate_replay_scope_token(label: str, value: object) -> None:
    text = str(value or "")
    stripped = text.strip()
    if not stripped:
        raise ValueError(f"{label} must be non-empty")
    if text != stripped:
        raise ValueError(f"{label} must not include surrounding whitespace")
    if any(char.isspace() for char in stripped):
        raise ValueError(f"{label} must not include whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in stripped):
        raise ValueError(f"{label} must not contain control characters")


def validate_message_type_token(label: str, value: object) -> None:
    text = str(value or "")
    stripped = text.strip()
    if not stripped:
        raise ValueError(f"{label} must be non-empty")
    if text != stripped:
        raise ValueError(f"{label} must not include surrounding whitespace")
    if any(char.isspace() for char in stripped):
        raise ValueError(f"{label} must not include whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in stripped):
        raise ValueError(f"{label} must not contain control characters")


def validate_upsert_payload(request: UpsertRequest) -> dict[str, object]:
    if not request.record_json:
        raise ValueError("Upsert proof requires non-empty record_json")
    try:
        decoded = request.record_json.decode("utf-8")
        payload = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Upsert proof record_json must be a valid JSON object") from error
    except ValueError as error:
        if "non-standard constant" in str(error):
            raise ValueError(f"Upsert proof record_json must not contain non-standard JSON constants: {error}") from error
        raise ValueError(f"Upsert proof record_json must not contain duplicate JSON keys: {error}") from error
    if not isinstance(payload, dict):
        raise ValueError("Upsert proof record_json must be a JSON object")
    if not payload:
        raise ValueError("Upsert proof record_json must be a non-empty JSON object")
    return payload


def validate_delete_filter(request: DeleteRequest) -> None:
    if not request.filter.fields:
        raise ValueError("Delete proof requires a non-empty filter")
    for field_name, value in request.filter.fields.items():
        field_text = str(field_name or "")
        stripped = field_text.strip()
        if not stripped:
            raise ValueError("Delete proof filter field names must be non-empty")
        if field_text != stripped:
            raise ValueError("Delete proof filter field names must not include surrounding whitespace")
        if any(char.isspace() for char in stripped):
            raise ValueError("Delete proof filter field names must not include whitespace")
        if any(ord(char) < 32 or ord(char) == 127 for char in stripped):
            raise ValueError("Delete proof filter field names must not contain control characters")
        if value.WhichOneof("kind") == "null_value":
            raise ValueError("Delete proof filter values must not be null")


def _delete_filter_value_matches_payload(payload_value: object, filter_value) -> bool:
    kind = filter_value.WhichOneof("kind")
    if kind == "string_value":
        return isinstance(payload_value, str) and payload_value == filter_value.string_value
    if kind == "number_value":
        return (
            isinstance(payload_value, (int, float))
            and not isinstance(payload_value, bool)
            and float(payload_value) == filter_value.number_value
        )
    if kind == "bool_value":
        return isinstance(payload_value, bool) and payload_value == filter_value.bool_value
    return False


def validate_delete_filter_matches_upsert_payload(upsert: UpsertRequest, delete: DeleteRequest) -> None:
    payload = validate_upsert_payload(upsert)
    validate_delete_filter(delete)
    for field_name, filter_value in delete.filter.fields.items():
        if field_name in payload and _delete_filter_value_matches_payload(payload[field_name], filter_value):
            return
    raise ValueError(
        "Upsert/Delete replay proofs must share at least one Delete filter field/value with Upsert record_json"
    )


def validate_shared_replay_scope(upsert: UpsertRequest, delete: DeleteRequest) -> None:
    mismatches: list[str] = []
    if upsert.context.tenant_id != delete.context.tenant_id:
        mismatches.append("tenant_id")
    if upsert.context.project_id != delete.context.project_id:
        mismatches.append("project_id")
    if upsert.message_type != delete.message_type:
        mismatches.append("message_type")
    if upsert.idempotency_key != delete.idempotency_key:
        mismatches.append("idempotency_key")
    if mismatches:
        raise ValueError(f"Upsert/Delete replay proofs must share {', '.join(mismatches)}")


def assert_retry_metadata_gate(upsert_request: UpsertRequest, delete_request: DeleteRequest) -> None:
    assert_databroker_method_request(UPSERT_METHOD, upsert_request, "Upsert")
    assert_databroker_method_request(DELETE_METHOD, delete_request, "Delete")
    if RPC_REPLAY_SAFE.get(UPSERT_METHOD) != "true":
        raise AssertionError("DataBroker.Upsert must be generated as replay-safe")
    if RPC_REPLAY_SAFE.get(DELETE_METHOD) != "true":
        raise AssertionError("DataBroker.Delete must be generated as replay-safe")
    if not _is_replay_safe(UPSERT_METHOD):
        raise AssertionError("_is_replay_safe rejected DataBroker.Upsert")
    if not _is_replay_safe(DELETE_METHOD):
        raise AssertionError("_is_replay_safe rejected DataBroker.Delete")
    if _is_replay_safe(DOCUMENT_UPSERT_METHOD):
        raise AssertionError("DocumentUpsert must not be replay-safe without a durable duplicate contract")
    if not _request_has_idempotency_key(upsert_request):
        raise AssertionError("replay-safe Upsert proof requires a non-empty idempotency_key")
    if not _request_has_idempotency_key(delete_request):
        raise AssertionError("replay-safe Delete proof requires a non-empty idempotency_key")

    without_key = _clone_request(upsert_request)
    without_key.idempotency_key = ""
    if _request_has_idempotency_key(without_key):
        raise AssertionError("empty top-level idempotency_key must not satisfy the retry gate")

    policy = RetryPolicy(max_attempts=2, initial_backoff=0.0, jitter=0.0)
    if not policy.should_retry(
        grpc.StatusCode.UNAVAILABLE,
        1,
        read_only=False,
        replay_safe=True,
        has_idempotency_key=True,
    ):
        raise AssertionError("replay-safe keyed mutation should retry UNAVAILABLE")
    if policy.should_retry(
        grpc.StatusCode.UNAVAILABLE,
        1,
        read_only=False,
        replay_safe=True,
        has_idempotency_key=False,
    ):
        raise AssertionError("replay-safe mutation without idempotency key must not retry")
    if policy.should_retry(
        grpc.StatusCode.UNAVAILABLE,
        1,
        read_only=False,
        replay_safe=False,
        has_idempotency_key=True,
    ):
        raise AssertionError("non-replay-safe mutation must not retry even with idempotency key")
    if policy.should_retry(
        grpc.StatusCode.DEADLINE_EXCEEDED,
        1,
        read_only=False,
        replay_safe=True,
        has_idempotency_key=True,
    ):
        raise AssertionError("mutation DEADLINE_EXCEEDED must not be auto-retried")


def make_stub(target: str, tls: bool):
    if tls:
        channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())
    else:
        channel = grpc.insecure_channel(target)
    return DataBrokerStub(channel)


def _assert_restored_summary(
    label: str,
    first,
    second,
    fields: tuple[str, ...],
    expected_resource_authority: str | None = None,
    expected_resource_path_prefix: str | None = None,
) -> None:
    restored = False
    for field in fields:
        first_value = getattr(first, field)
        if not _summary_field_has_value(
            label,
            field,
            first_value,
            expected_resource_authority,
            expected_resource_path_prefix,
        ):
            continue
        restored = True
        second_value = getattr(second, field)
        if second_value != first_value:
            raise AssertionError(f"{label}: duplicate replay {field} differs from first response")
        if field == "write_receipt_json":
            _assert_typed_write_receipt_lockstep(label, first, first_value, "first response")
            _assert_typed_write_receipt_lockstep(label, second, second_value, "duplicate response")
            if second.write_receipt != first.write_receipt:
                raise AssertionError(f"{label}: duplicate replay typed write_receipt differs from first response")
    if not restored:
        raise AssertionError(f"{label}: first response must include at least one replay summary field")


def _assert_mutation_id(label: str, response_label: str, response: MutationResponse) -> None:
    mutation_id = response.mutation_id
    if not mutation_id:
        raise AssertionError(f"{label}: {response_label} mutation_id must be non-empty")
    if mutation_id != mutation_id.strip():
        raise AssertionError(f"{label}: {response_label} mutation_id must not include surrounding whitespace")
    if any(char.isspace() for char in mutation_id):
        raise AssertionError(f"{label}: {response_label} mutation_id must not include whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in mutation_id):
        raise AssertionError(f"{label}: {response_label} mutation_id must not contain control characters")
    if not MUTATION_ID_PATTERN.fullmatch(mutation_id):
        raise AssertionError(f"{label}: {response_label} mutation_id must be a canonical lowercase UUID")


def _summary_field_has_value(
    label: str,
    field: str,
    value,
    expected_resource_authority: str | None = None,
    expected_resource_path_prefix: str | None = None,
) -> bool:
    if not value:
        return False
    if isinstance(value, (bytes, bytearray)):
        stripped = bytes(value).strip()
        if not stripped:
            raise AssertionError(f"{label}: first response {field} must not be whitespace-only")
        if bytes(value) != stripped:
            raise AssertionError(f"{label}: first response {field} must not include surrounding whitespace")
        if field == "record_json":
            _assert_summary_json_object(label, field, stripped)
        return True
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise AssertionError(f"{label}: first response {field} must not be whitespace-only")
        if value != stripped:
            raise AssertionError(f"{label}: first response {field} must not include surrounding whitespace")
        if field == "resource_uri":
            _assert_summary_resource_uri(label, stripped, expected_resource_authority, expected_resource_path_prefix)
        if field == "checksum_sha256" and not MANIFEST_CHECKSUM_PATTERN.fullmatch(stripped):
            raise AssertionError(f"{label}: first response checksum_sha256 must be sha256:<64 lowercase hex>")
        if field == "write_receipt_json":
            receipt = _assert_summary_json_object(label, field, stripped)
            _assert_summary_write_receipt_json(label, receipt)
        return True
    return True


def _assert_summary_resource_uri(
    label: str,
    value: str,
    expected_authority: str | None = None,
    expected_path_prefix: str | None = None,
) -> None:
    prefix = "udb://"
    if not value.startswith(prefix):
        raise AssertionError(f"{label}: first response resource_uri must start with {prefix}")
    rest = value[len(prefix):]
    if not rest or "/" not in rest:
        raise AssertionError(f"{label}: first response resource_uri must include non-empty authority and path")
    authority, path = rest.split("/", 1)
    if not authority or not path:
        raise AssertionError(f"{label}: first response resource_uri must include non-empty authority and path")
    if any(char.isspace() for char in value):
        raise AssertionError(f"{label}: first response resource_uri must not include whitespace")
    if expected_authority is not None and authority != expected_authority:
        raise AssertionError(
            f"{label}: first response resource_uri authority must equal request tenant_id {expected_authority!r}"
        )
    first_segment = path.split("/", 1)[0]
    if expected_path_prefix is not None and first_segment != expected_path_prefix:
        raise AssertionError(
            f"{label}: first response resource_uri path must start with request message_type {expected_path_prefix!r}"
        )
    if expected_path_prefix is not None:
        segments = path.split("/", 1)
        if len(segments) != 2 or not segments[1]:
            raise AssertionError(
                f"{label}: first response resource_uri path must include request message_type and resource id"
            )


def _resource_id_candidates_from_payload(payload: dict[str, object]) -> set[str]:
    identity_candidates: set[str] = set()
    for key, value in payload.items():
        is_identity_field = key == "id" or key.endswith("_id")
        if isinstance(value, str) and value:
            if is_identity_field:
                stripped = value.strip()
                if not stripped:
                    raise AssertionError("resource_uri id proof identity field value must be non-empty")
                if value != stripped:
                    raise AssertionError(
                        "resource_uri id proof identity field value must not include surrounding whitespace"
                    )
                if any(char.isspace() for char in stripped):
                    raise AssertionError("resource_uri id proof identity field value must not include whitespace")
            if is_identity_field:
                identity_candidates.add(value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            if is_identity_field:
                identity_candidates.add(str(value))
        elif is_identity_field:
            raise AssertionError("resource_uri id proof identity field value must be non-empty")
    return identity_candidates


def _resource_id_candidates_from_delete_filter(request: DeleteRequest) -> set[str]:
    identity_candidates: set[str] = set()
    for field_name, value in request.filter.fields.items():
        is_identity_field = field_name == "id" or field_name.endswith("_id")
        kind = value.WhichOneof("kind")
        if kind == "string_value" and value.string_value:
            if is_identity_field:
                stripped = value.string_value.strip()
                if not stripped:
                    raise AssertionError("resource_uri id proof identity field value must be non-empty")
                if value.string_value != stripped:
                    raise AssertionError(
                        "resource_uri id proof identity field value must not include surrounding whitespace"
                    )
                if any(char.isspace() for char in stripped):
                    raise AssertionError("resource_uri id proof identity field value must not include whitespace")
            if is_identity_field:
                identity_candidates.add(value.string_value)
        elif kind == "number_value":
            if is_identity_field:
                identity_candidates.add(str(value.number_value))
        elif is_identity_field:
            raise AssertionError("resource_uri id proof identity field value must be non-empty")
    return identity_candidates


def _assert_summary_resource_uri_matches_candidates(
    label: str,
    resource_uri: str,
    candidates: set[str],
) -> None:
    if not resource_uri:
        return
    if not candidates:
        raise AssertionError(f"{label}: resource_uri id proof requires at least one scalar identity request field")
    path = resource_uri[len("udb://"):].split("/", 1)[1]
    resource_id = path.split("/", 1)[1].split("/", 1)[0]
    if resource_id not in candidates:
        raise AssertionError(f"{label}: first response resource_uri id must match an identity request field value")


def _assert_summary_json_object(label: str, field: str, value: bytes | str) -> dict:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
        out: dict[str, object] = {}
        for key, pair_value in pairs:
            if key in out:
                raise AssertionError(f"{label}: first response {field} must not contain duplicate JSON key {key!r}")
            out[key] = pair_value
        return out

    try:
        if isinstance(value, bytes):
            decoded = json.loads(
                value.decode("utf-8"),
                object_pairs_hook=reject_duplicate_keys,
                parse_constant=_reject_non_finite_json_constant,
            )
        else:
            decoded = json.loads(
                value,
                object_pairs_hook=reject_duplicate_keys,
                parse_constant=_reject_non_finite_json_constant,
            )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AssertionError(f"{label}: first response {field} must be a valid JSON object") from error
    except ValueError as error:
        if "non-standard constant" in str(error):
            raise AssertionError(f"{label}: first response {field} must not contain non-standard JSON constants") from error
        raise
    if not isinstance(decoded, dict):
        raise AssertionError(f"{label}: first response {field} must be a JSON object")
    if not decoded:
        raise AssertionError(f"{label}: first response {field} must be a non-empty JSON object")
    return decoded


def _assert_upsert_record_summary_matches_request(request: UpsertRequest, first) -> None:
    if not first.record_json:
        return
    request_payload = validate_upsert_payload(request)
    response_payload = _assert_summary_json_object("Upsert", "record_json", first.record_json)
    missing_or_different = [
        key
        for key, value in request_payload.items()
        if key not in response_payload or response_payload[key] != value
    ]
    if missing_or_different:
        raise AssertionError(
            f"Upsert: first response record_json must include request field/value(s) {missing_or_different!r}"
        )


def _assert_summary_write_receipt_json(label: str, receipt: dict) -> None:
    required_fields = ("source_lsn", "outbox_seq", "projection_task_ids", "manifest_checksum", "written_at_unix_ms")
    missing = [field for field in required_fields if field not in receipt]
    if missing:
        raise AssertionError(f"{label}: first response write_receipt_json missing fields {missing!r}")
    unexpected = sorted(field for field in receipt if field not in required_fields)
    if unexpected:
        raise AssertionError(f"{label}: first response write_receipt_json unexpected fields {unexpected!r}")
    source_lsn = receipt["source_lsn"]
    if not isinstance(source_lsn, str):
        raise AssertionError(f"{label}: first response write_receipt_json source_lsn must be a string")
    if not source_lsn:
        raise AssertionError(f"{label}: first response write_receipt_json source_lsn must be non-empty")
    if source_lsn != source_lsn.strip():
        raise AssertionError(f"{label}: first response write_receipt_json source_lsn must not include surrounding whitespace")
    if any(char.isspace() for char in source_lsn):
        raise AssertionError(f"{label}: first response write_receipt_json source_lsn must not include whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in source_lsn):
        raise AssertionError(f"{label}: first response write_receipt_json source_lsn must not contain control characters")
    outbox_seq = receipt["outbox_seq"]
    if not isinstance(outbox_seq, int) or isinstance(outbox_seq, bool) or outbox_seq < 0:
        raise AssertionError(f"{label}: first response write_receipt_json outbox_seq must be a non-negative integer")
    projection_task_ids = receipt["projection_task_ids"]
    if not isinstance(projection_task_ids, list):
        raise AssertionError(f"{label}: first response write_receipt_json projection_task_ids must be an array")
    for index, task_id in enumerate(projection_task_ids):
        if not isinstance(task_id, str) or not task_id or task_id != task_id.strip():
            raise AssertionError(
                f"{label}: first response write_receipt_json projection_task_ids[{index}] must be a non-empty unpadded string"
            )
        if any(char.isspace() for char in task_id):
            raise AssertionError(
                f"{label}: first response write_receipt_json projection_task_ids[{index}] must not include whitespace"
            )
        if any(ord(char) < 32 or ord(char) == 127 for char in task_id):
            raise AssertionError(
                f"{label}: first response write_receipt_json projection_task_ids[{index}] must not contain control characters"
            )
    manifest_checksum = receipt["manifest_checksum"]
    if not isinstance(manifest_checksum, str) or not manifest_checksum.strip():
        raise AssertionError(f"{label}: first response write_receipt_json manifest_checksum must be a non-empty string")
    if manifest_checksum != manifest_checksum.strip():
        raise AssertionError(f"{label}: first response write_receipt_json manifest_checksum must not include surrounding whitespace")
    if not MANIFEST_CHECKSUM_PATTERN.fullmatch(manifest_checksum):
        raise AssertionError(
            f"{label}: first response write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>"
        )
    written_at_unix_ms = receipt["written_at_unix_ms"]
    if not isinstance(written_at_unix_ms, int) or isinstance(written_at_unix_ms, bool) or written_at_unix_ms <= 0:
        raise AssertionError(f"{label}: first response write_receipt_json written_at_unix_ms must be a positive integer")


def _assert_typed_write_receipt_lockstep(
    label: str,
    response: MutationResponse,
    receipt_json: str,
    response_label: str,
) -> None:
    receipt = _assert_summary_json_object(label, "write_receipt_json", receipt_json)
    _assert_summary_write_receipt_json(label, receipt)
    if not response.HasField("write_receipt"):
        raise AssertionError(f"{label}: {response_label} typed write_receipt must be present when write_receipt_json is present")
    typed = response.write_receipt
    typed_receipt = {
        "source_lsn": typed.source_lsn,
        "outbox_seq": int(typed.outbox_seq),
        "projection_task_ids": list(typed.projection_task_ids),
        "manifest_checksum": typed.manifest_checksum,
        "written_at_unix_ms": int(typed.written_at_unix_ms),
    }
    if typed_receipt != receipt:
        raise AssertionError(f"{label}: {response_label} typed write_receipt must match write_receipt_json")


def check_served_replay(stub, request: UpsertRequest, metadata, timeout: float) -> None:
    request = validate_runtime_upsert_request("Upsert replay proof", request)
    assert_databroker_method_request(UPSERT_METHOD, request, "Upsert")
    runtime_metadata, runtime_timeout = validate_runtime_transport_inputs("Upsert replay proof", metadata, timeout)
    upsert = validate_runtime_stub_method("Upsert replay proof", stub, "Upsert")
    request_payload = validate_upsert_payload(request)
    resource_id_candidates = _resource_id_candidates_from_payload(request_payload)
    first = call_runtime_mutation(
        "Upsert replay proof first response",
        upsert,
        request,
        runtime_metadata,
        runtime_timeout,
    )
    _assert_mutation_id("Upsert", "first response", first)
    if first.was_duplicate:
        raise AssertionError("first replay-safe Upsert returned was_duplicate=true")
    if first.affected_rows <= 0:
        raise AssertionError(f"first replay-safe Upsert affected_rows must be positive, got {first.affected_rows}")
    second = call_runtime_mutation(
        "Upsert replay proof second response",
        upsert,
        request,
        runtime_metadata,
        runtime_timeout,
    )
    _assert_mutation_id("Upsert", "duplicate response", second)
    if not second.was_duplicate:
        raise AssertionError("second replay-safe Upsert did not return was_duplicate=true")
    if second.mutation_id != first.mutation_id:
        raise AssertionError("duplicate replay mutation_id differs from first response")
    if second.affected_rows != first.affected_rows:
        raise AssertionError("duplicate replay affected_rows differs from first response")
    _assert_restored_summary(
        "Upsert",
        first,
        second,
        ("record_json", "resource_uri", "checksum_sha256", "write_receipt_json"),
        request.context.tenant_id,
        request.message_type,
    )
    _assert_upsert_record_summary_matches_request(request, first)
    _assert_summary_resource_uri_matches_candidates(
        "Upsert",
        first.resource_uri,
        resource_id_candidates,
    )


def check_served_delete_replay(stub, request: DeleteRequest, metadata, timeout: float) -> None:
    request = validate_runtime_delete_request("Delete replay proof", request)
    assert_databroker_method_request(DELETE_METHOD, request, "Delete")
    runtime_metadata, runtime_timeout = validate_runtime_transport_inputs("Delete replay proof", metadata, timeout)
    delete = validate_runtime_stub_method("Delete replay proof", stub, "Delete")
    validate_delete_filter(request)
    resource_id_candidates = _resource_id_candidates_from_delete_filter(request)
    first = call_runtime_mutation(
        "Delete replay proof first response",
        delete,
        request,
        runtime_metadata,
        runtime_timeout,
    )
    _assert_mutation_id("Delete", "first response", first)
    if first.was_duplicate:
        raise AssertionError("first replay-safe Delete returned was_duplicate=true")
    if first.affected_rows <= 0:
        raise AssertionError(f"first replay-safe Delete affected_rows must be positive, got {first.affected_rows}")
    second = call_runtime_mutation(
        "Delete replay proof second response",
        delete,
        request,
        runtime_metadata,
        runtime_timeout,
    )
    _assert_mutation_id("Delete", "duplicate response", second)
    if not second.was_duplicate:
        raise AssertionError("second replay-safe Delete did not return was_duplicate=true")
    if second.mutation_id != first.mutation_id:
        raise AssertionError("duplicate Delete replay mutation_id differs from first response")
    if second.affected_rows != first.affected_rows:
        raise AssertionError("duplicate Delete replay affected_rows differs from first response")
    _assert_restored_summary(
        "Delete",
        first,
        second,
        ("resource_uri", "write_receipt_json"),
        request.context.tenant_id,
        request.message_type,
    )
    _assert_summary_resource_uri_matches_candidates(
        "Delete",
        first.resource_uri,
        resource_id_candidates,
    )


class _FakeStub:
    def __init__(self) -> None:
        self.upsert_calls = 0
        self.delete_calls = 0

    def Upsert(self, request, metadata=None, timeout=None):
        self.upsert_calls += 1
        return _mutation_response(
            resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
            record_json=request.record_json,
            affected_rows=1,
            was_duplicate=self.upsert_calls == 2,
            write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
            write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
        )

    def Delete(self, request, metadata=None, timeout=None):
        self.delete_calls += 1
        return _mutation_response(
            resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
            affected_rows=1,
            was_duplicate=self.delete_calls == 2,
            write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
            write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
        )


def run_selftest() -> None:
    req = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    req.context.tenant_id = "tenant-a"
    req.context.project_id = "project-a"
    req.record_json = b'{"id":"rec-1","invoice_id":"rec-1","total_cents":100}'
    delete_req = DeleteRequest(message_type="Invoice", idempotency_key="idem-1")
    delete_req.context.tenant_id = "tenant-a"
    delete_req.context.project_id = "project-a"
    delete_req.filter["invoice_id"] = "rec-1"
    validate_replay_request("Upsert", req)
    validate_replay_request("Delete", delete_req)
    validate_shared_replay_scope(req, delete_req)
    validate_delete_filter_matches_upsert_payload(req, delete_req)
    assert_retry_metadata_gate(req, delete_req)
    try:
        assert_databroker_method_request(UPSERT_METHOD, delete_req, "Upsert")
    except AssertionError as error:
        if "does not match RPC input" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe method/request descriptor mismatch regression was not caught")
    try:
        assert_databroker_method_request("/udb.services.v1.DataBroker/Missing", req, "Missing")
    except AssertionError as error:
        if "DataBroker generated descriptor has no method Missing" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe missing method descriptor regression was not caught")
    stub = _FakeStub()
    check_served_replay(stub, req, (), 1.0)
    check_served_delete_replay(stub, delete_req, (), 1.0)
    try:
        check_served_replay(_FakeStub(), object(), (), 1.0)  # type: ignore[arg-type]
    except ValueError as error:
        if "runtime request must be an UpsertRequest" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Upsert request-message validation regression was not caught")
    try:
        check_served_delete_replay(_FakeStub(), object(), (), 1.0)  # type: ignore[arg-type]
    except ValueError as error:
        if "runtime request must be a DeleteRequest" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Delete request-message validation regression was not caught")
    try:
        check_served_replay(_FakeStub(), req, [], 1.0)
    except ValueError as error:
        if "runtime metadata must be a parsed gRPC metadata tuple" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime metadata validation regression was not caught")
    try:
        check_served_delete_replay(_FakeStub(), delete_req, (), "1e2")  # type: ignore[arg-type]
    except ValueError as error:
        if "runtime timeout is invalid" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime timeout validation regression was not caught")
    try:
        check_served_replay(object(), req, (), 1.0)
    except ValueError as error:
        if "runtime stub must expose callable Upsert" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Upsert stub validation regression was not caught")
    try:
        check_served_delete_replay(object(), delete_req, (), 1.0)
    except ValueError as error:
        if "runtime stub must expose callable Delete" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Delete stub validation regression was not caught")

    class NonResponseUpsertStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            return object()

    try:
        check_served_replay(NonResponseUpsertStub(), req, (), 1.0)
    except AssertionError as error:
        if "runtime response must be a MutationResponse" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Upsert response-message validation regression was not caught")

    class NonResponseDeleteStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            return object()

    try:
        check_served_delete_replay(NonResponseDeleteStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "runtime response must be a MutationResponse" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Delete response-message validation regression was not caught")

    class FailingUpsertStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            raise RuntimeError("upsert exploded")

    try:
        check_served_replay(FailingUpsertStub(), req, (), 1.0)
    except AssertionError as error:
        if "runtime call raised error" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Upsert call-error validation regression was not caught")

    class RpcErrorUpsertStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            raise grpc.RpcError("upsert unavailable")

    try:
        check_served_replay(RpcErrorUpsertStub(), req, (), 1.0)
    except AssertionError as error:
        if "runtime call raised unexpected gRPC error" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Upsert unexpected-RpcError validation regression was not caught")

    class FailingDeleteStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            raise RuntimeError("delete exploded")

    try:
        check_served_delete_replay(FailingDeleteStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "runtime call raised error" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Delete call-error validation regression was not caught")

    class RpcErrorDeleteStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            raise grpc.RpcError("delete unavailable")

    try:
        check_served_delete_replay(RpcErrorDeleteStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "runtime call raised unexpected gRPC error" not in str(error):
            raise
    else:
        raise AssertionError("retry-safe runtime Delete unexpected-RpcError validation regression was not caught")

    class BadUpsertAffectedRowsReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=0 if self.upsert_calls > 1 else 1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_replay(BadUpsertAffectedRowsReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "duplicate replay affected_rows differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("Upsert affected_rows replay regression was not caught")

    class BadUpsertMutationIdReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                mutation_id=(
                    "22222222-2222-4222-8222-222222222222"
                    if self.upsert_calls > 1
                    else "11111111-1111-4111-8111-111111111111"
                ),
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_replay(BadUpsertMutationIdReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "duplicate replay mutation_id differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("Upsert mutation_id replay regression was not caught")

    class AddedUpsertMutationIdReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                mutation_id="22222222-2222-4222-8222-222222222222" if self.upsert_calls > 1 else "",
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_replay(AddedUpsertMutationIdReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "Upsert: first response mutation_id must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("Upsert added mutation_id replay regression was not caught")

    class InvalidUpsertMutationIdShapeStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                mutation_id="not-a-uuid",
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_replay(InvalidUpsertMutationIdShapeStub(), req, (), 1.0)
    except AssertionError as error:
        if "Upsert: first response mutation_id must be a canonical lowercase UUID" not in str(error):
            raise
    else:
        raise AssertionError("Upsert invalid mutation_id shape regression was not caught")

    class BadUpsertFreshAffectedRowsStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=0,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_replay(BadUpsertFreshAffectedRowsStub(), req, (), 1.0)
    except AssertionError as error:
        if "first replay-safe Upsert affected_rows must be positive" not in str(error):
            raise
    else:
        raise AssertionError("Upsert fresh affected_rows regression was not caught")

    class EmptyUpsertSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(EmptyUpsertSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response must include at least one replay summary field" not in str(error):
            raise
    else:
        raise AssertionError("Upsert empty replay summary regression was not caught")

    class InvalidUpsertResourceUriSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(InvalidUpsertResourceUriSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri must start with udb://" not in str(error):
            raise
    else:
        raise AssertionError("Upsert invalid resource_uri replay summary regression was not caught")

    class WrongTenantUpsertResourceUriSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://other-tenant/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(WrongTenantUpsertResourceUriSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri authority must equal request tenant_id" not in str(error):
            raise
    else:
        raise AssertionError("Upsert wrong-tenant resource_uri replay summary regression was not caught")

    class WrongMessageUpsertResourceUriSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/OtherMessage/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(WrongMessageUpsertResourceUriSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri path must start with request message_type" not in str(error):
            raise
    else:
        raise AssertionError("Upsert wrong-message resource_uri replay summary regression was not caught")

    class ShortPathUpsertResourceUriSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(ShortPathUpsertResourceUriSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri path must include request message_type and resource id" not in str(error):
            raise
    else:
        raise AssertionError("Upsert short-path resource_uri replay summary regression was not caught")

    class WrongIdUpsertResourceUriSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/other-rec",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(WrongIdUpsertResourceUriSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri id must match an identity request field value" not in str(error):
            raise
    else:
        raise AssertionError("Upsert wrong-id resource_uri replay summary regression was not caught")

    class NonIdentityScalarUpsertResourceUriSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/100",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(NonIdentityScalarUpsertResourceUriSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri id must match an identity request field value" not in str(error):
            raise
    else:
        raise AssertionError("Upsert non-identity scalar resource_uri replay summary regression was not caught")

    missing_identity_upsert = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    missing_identity_upsert.context.tenant_id = "tenant-a"
    missing_identity_upsert.context.project_id = "project-a"
    missing_identity_upsert.record_json = b'{"total_cents":100}'

    class MissingIdentityUpsertResourceUriSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/100",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(MissingIdentityUpsertResourceUriSummaryReplayStub(), missing_identity_upsert, (), 1.0)
    except AssertionError as error:
        if "resource_uri id proof requires at least one scalar identity request field" not in str(error):
            raise
    else:
        raise AssertionError("Upsert missing identity resource_uri replay summary regression was not caught")

    padded_identity_upsert = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    padded_identity_upsert.context.tenant_id = "tenant-a"
    padded_identity_upsert.context.project_id = "project-a"
    padded_identity_upsert.record_json = b'{"id":" rec-1 ","invoice_id":"rec-1","total_cents":100}'
    try:
        check_served_replay(_FakeStub(), padded_identity_upsert, (), 1.0)
    except AssertionError as error:
        if "identity field value must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("Upsert padded identity resource_uri replay summary regression was not caught")

    embedded_space_identity_upsert = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    embedded_space_identity_upsert.context.tenant_id = "tenant-a"
    embedded_space_identity_upsert.context.project_id = "project-a"
    embedded_space_identity_upsert.record_json = b'{"id":"rec 1","invoice_id":"rec-1","total_cents":100}'
    try:
        check_served_replay(_FakeStub(), embedded_space_identity_upsert, (), 1.0)
    except AssertionError as error:
        if "identity field value must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("Upsert embedded-space identity resource_uri replay summary regression was not caught")

    class WhitespaceUpsertSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                record_json=b"   ",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(WhitespaceUpsertSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must not be whitespace-only" not in str(error):
            raise
    else:
        raise AssertionError("Upsert whitespace replay summary regression was not caught")

    class MalformedUpsertRecordSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                record_json=b"not-json",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(MalformedUpsertRecordSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must be a valid JSON object" not in str(error):
            raise
    else:
        raise AssertionError("Upsert malformed record_json replay summary regression was not caught")

    class NonFiniteUpsertRecordSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                record_json=b'{"id":NaN}',
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(NonFiniteUpsertRecordSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must not contain non-standard JSON constants" not in str(error):
            raise
    else:
        raise AssertionError("Upsert non-finite record_json replay summary regression was not caught")

    class DuplicateKeyUpsertRecordSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                record_json=b'{"id":"1","id":"2"}',
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(DuplicateKeyUpsertRecordSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must not contain duplicate JSON key" not in str(error):
            raise
    else:
        raise AssertionError("Upsert duplicate-key record_json replay summary regression was not caught")

    class MissingTypedUpsertReceiptReplaySummaryStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
            )

    try:
        check_served_replay(MissingTypedUpsertReceiptReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "Upsert: first response typed write_receipt must be present when write_receipt_json is present" not in str(error):
            raise
    else:
        raise AssertionError("Upsert missing typed write_receipt replay summary regression was not caught")

    mismatched_write_receipt = {
        **SUMMARY_WRITE_RECEIPT_DICT,
        "manifest_checksum": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
    }

    class MismatchedTypedUpsertReceiptReplaySummaryStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=mismatched_write_receipt,
            )

    try:
        check_served_replay(MismatchedTypedUpsertReceiptReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "Upsert: first response typed write_receipt must match write_receipt_json" not in str(error):
            raise
    else:
        raise AssertionError("Upsert mismatched typed write_receipt replay summary regression was not caught")

    class UnexpectedFieldUpsertReceiptSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1700000000000,"shadow_fence":"leak"}'
                ),
            )

    try:
        check_served_replay(UnexpectedFieldUpsertReceiptSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json unexpected fields ['shadow_fence']" not in str(error):
            raise
    else:
        raise AssertionError("Upsert unexpected-field write_receipt_json replay summary regression was not caught")

    class EmptySourceLsnUpsertReceiptSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1700000000000}'
                ),
            )

    try:
        check_served_replay(EmptySourceLsnUpsertReceiptSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json source_lsn must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("Upsert empty source_lsn write_receipt_json replay summary regression was not caught")

    class ControlSourceLsnUpsertReceiptSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1\\u0000","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1700000000000}'
                ),
            )

    try:
        check_served_replay(ControlSourceLsnUpsertReceiptSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json source_lsn must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("Upsert control-character source_lsn write_receipt_json replay summary regression was not caught")

    class WhitespaceProjectionTaskUpsertReceiptSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":["task 1"],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1700000000000}'
                ),
            )

    try:
        check_served_replay(WhitespaceProjectionTaskUpsertReceiptSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json projection_task_ids[0] must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError(
            "Upsert whitespace projection_task_ids write_receipt_json replay summary regression was not caught"
        )

    class ControlProjectionTaskUpsertReceiptSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":["task-1\\u0000"],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1700000000000}'
                ),
            )

    try:
        check_served_replay(ControlProjectionTaskUpsertReceiptSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json projection_task_ids[0] must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError(
            "Upsert control-character projection_task_ids write_receipt_json replay summary regression was not caught"
        )

    class InvalidChecksumUpsertReceiptSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:test","written_at_unix_ms":1700000000000}'
                ),
            )

    try:
        check_served_replay(InvalidChecksumUpsertReceiptSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>" not in str(error):
            raise
    else:
        raise AssertionError("Upsert invalid manifest_checksum write_receipt_json replay summary regression was not caught")

    class WrongUpsertRecordSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                record_json=b'{"id":"other","invoice_id":"invoice-404","total_cents":100}',
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_served_replay(WrongUpsertRecordSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must include request field/value" not in str(error):
            raise
    else:
        raise AssertionError("Upsert mismatched record_json replay summary regression was not caught")

    class InvalidUpsertChecksumSummaryReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                checksum_sha256="sha256:test",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_replay(InvalidUpsertChecksumSummaryReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response checksum_sha256 must be sha256:<64 lowercase hex>" not in str(error):
            raise
    else:
        raise AssertionError("Upsert invalid checksum_sha256 replay summary regression was not caught")

    class BadUpsertChecksumReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                checksum_sha256=(
                    "sha256:2222222222222222222222222222222222222222222222222222222222222222"
                    if self.upsert_calls > 1
                    else "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                ),
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_replay(BadUpsertChecksumReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "duplicate replay checksum_sha256 differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("Upsert checksum replay regression was not caught")

    class DroppedUpsertReceiptReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json="" if self.upsert_calls > 1 else SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_replay(DroppedUpsertReceiptReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "duplicate replay write_receipt_json differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("Upsert dropped replay summary regression was not caught")

    class BadDeleteAffectedRowsReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=0 if self.delete_calls > 1 else 1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_delete_replay(BadDeleteAffectedRowsReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "duplicate Delete replay affected_rows differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("Delete affected_rows replay regression was not caught")

    class BadDeleteMutationIdReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                mutation_id=(
                    "22222222-2222-4222-8222-222222222222"
                    if self.delete_calls > 1
                    else "11111111-1111-4111-8111-111111111111"
                ),
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_delete_replay(BadDeleteMutationIdReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "duplicate Delete replay mutation_id differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("Delete mutation_id replay regression was not caught")

    class AddedDeleteMutationIdReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                mutation_id="22222222-2222-4222-8222-222222222222" if self.delete_calls > 1 else "",
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_delete_replay(AddedDeleteMutationIdReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "Delete: first response mutation_id must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("Delete added mutation_id replay regression was not caught")

    class InvalidDeleteMutationIdShapeStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                mutation_id="not-a-uuid",
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_delete_replay(InvalidDeleteMutationIdShapeStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "Delete: first response mutation_id must be a canonical lowercase UUID" not in str(error):
            raise
    else:
        raise AssertionError("Delete invalid mutation_id shape regression was not caught")

    class BadDeleteFreshAffectedRowsStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=0,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_delete_replay(BadDeleteFreshAffectedRowsStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first replay-safe Delete affected_rows must be positive" not in str(error):
            raise
    else:
        raise AssertionError("Delete fresh affected_rows regression was not caught")

    class EmptyDeleteSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(EmptyDeleteSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response must include at least one replay summary field" not in str(error):
            raise
    else:
        raise AssertionError("Delete empty replay summary regression was not caught")

    class PathlessDeleteResourceUriSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(PathlessDeleteResourceUriSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri must include non-empty authority and path" not in str(error):
            raise
    else:
        raise AssertionError("Delete pathless resource_uri replay summary regression was not caught")

    class WrongTenantDeleteResourceUriSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://other-tenant/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(WrongTenantDeleteResourceUriSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri authority must equal request tenant_id" not in str(error):
            raise
    else:
        raise AssertionError("Delete wrong-tenant resource_uri replay summary regression was not caught")

    class WrongMessageDeleteResourceUriSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/OtherMessage/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(WrongMessageDeleteResourceUriSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri path must start with request message_type" not in str(error):
            raise
    else:
        raise AssertionError("Delete wrong-message resource_uri replay summary regression was not caught")

    class ShortPathDeleteResourceUriSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(ShortPathDeleteResourceUriSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri path must include request message_type and resource id" not in str(error):
            raise
    else:
        raise AssertionError("Delete short-path resource_uri replay summary regression was not caught")

    class WrongIdDeleteResourceUriSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/other-rec",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(WrongIdDeleteResourceUriSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri id must match an identity request field value" not in str(error):
            raise
    else:
        raise AssertionError("Delete wrong-id resource_uri replay summary regression was not caught")

    non_identity_delete_req = DeleteRequest(message_type="Invoice", idempotency_key="idem-1")
    non_identity_delete_req.context.tenant_id = "tenant-a"
    non_identity_delete_req.context.project_id = "project-a"
    non_identity_delete_req.filter["invoice_id"] = "rec-1"
    non_identity_delete_req.filter["total_cents"] = 100

    class NonIdentityScalarDeleteResourceUriSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/100",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(NonIdentityScalarDeleteResourceUriSummaryReplayStub(), non_identity_delete_req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri id must match an identity request field value" not in str(error):
            raise
    else:
        raise AssertionError("Delete non-identity scalar resource_uri replay summary regression was not caught")

    missing_identity_delete_req = DeleteRequest(message_type="Invoice", idempotency_key="idem-1")
    missing_identity_delete_req.context.tenant_id = "tenant-a"
    missing_identity_delete_req.context.project_id = "project-a"
    missing_identity_delete_req.filter["total_cents"] = 100

    class MissingIdentityDeleteResourceUriSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/100",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(MissingIdentityDeleteResourceUriSummaryReplayStub(), missing_identity_delete_req, (), 1.0)
    except AssertionError as error:
        if "resource_uri id proof requires at least one scalar identity request field" not in str(error):
            raise
    else:
        raise AssertionError("Delete missing identity resource_uri replay summary regression was not caught")

    padded_identity_delete = DeleteRequest(message_type="Invoice", idempotency_key="idem-1")
    padded_identity_delete.context.tenant_id = "tenant-a"
    padded_identity_delete.context.project_id = "project-a"
    padded_identity_delete.filter["invoice_id"] = " rec-1 "
    try:
        check_served_delete_replay(_FakeStub(), padded_identity_delete, (), 1.0)
    except AssertionError as error:
        if "identity field value must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("Delete padded identity resource_uri replay summary regression was not caught")

    embedded_space_identity_delete = DeleteRequest(message_type="Invoice", idempotency_key="idem-1")
    embedded_space_identity_delete.context.tenant_id = "tenant-a"
    embedded_space_identity_delete.context.project_id = "project-a"
    embedded_space_identity_delete.filter["invoice_id"] = "rec 1"
    try:
        check_served_delete_replay(_FakeStub(), embedded_space_identity_delete, (), 1.0)
    except AssertionError as error:
        if "identity field value must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("Delete embedded-space identity resource_uri replay summary regression was not caught")

    class PaddedDeleteSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f" udb://{request.context.tenant_id or 'tenant'}/{request.message_type} ",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
            )

    try:
        check_served_delete_replay(PaddedDeleteSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("Delete padded replay summary regression was not caught")

    class MalformedDeleteReceiptSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json="not-json",
            )

    try:
        check_served_delete_replay(MalformedDeleteReceiptSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json must be a valid JSON object" not in str(error):
            raise
    else:
        raise AssertionError("Delete malformed write_receipt_json replay summary regression was not caught")

    class MissingFieldsDeleteReceiptSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json='{"lsn":"delete"}',
            )

    try:
        check_served_delete_replay(MissingFieldsDeleteReceiptSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json missing fields" not in str(error):
            raise
    else:
        raise AssertionError("Delete missing-fields write_receipt_json replay summary regression was not caught")

    class InvalidTimestampDeleteReceiptSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":0}'
                ),
            )

    try:
        check_served_delete_replay(InvalidTimestampDeleteReceiptSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json written_at_unix_ms must be a positive integer" not in str(error):
            raise
    else:
        raise AssertionError("Delete invalid timestamp write_receipt_json replay summary regression was not caught")

    class DuplicateKeyDeleteReceiptSummaryReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json='{"lsn":"1","lsn":"2"}',
            )

    try:
        check_served_delete_replay(DuplicateKeyDeleteReceiptSummaryReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json must not contain duplicate JSON key" not in str(error):
            raise
    else:
        raise AssertionError("Delete duplicate-key write_receipt_json replay summary regression was not caught")

    class MissingTypedDeleteReceiptReplaySummaryStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
            )

    try:
        check_served_delete_replay(MissingTypedDeleteReceiptReplaySummaryStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "Delete: first response typed write_receipt must be present when write_receipt_json is present" not in str(error):
            raise
    else:
        raise AssertionError("Delete missing typed write_receipt replay summary regression was not caught")

    class MismatchedTypedDeleteReceiptReplaySummaryStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=mismatched_write_receipt,
            )

    try:
        check_served_delete_replay(MismatchedTypedDeleteReceiptReplaySummaryStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "Delete: first response typed write_receipt must match write_receipt_json" not in str(error):
            raise
    else:
        raise AssertionError("Delete mismatched typed write_receipt replay summary regression was not caught")

    class DroppedDeleteReceiptReplayStub(_FakeStub):
        def Delete(self, request, metadata=None, timeout=None):
            self.delete_calls += 1
            return _mutation_response(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.delete_calls > 1,
                write_receipt_json="" if self.delete_calls > 1 else SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_served_delete_replay(DroppedDeleteReceiptReplayStub(), delete_req, (), 1.0)
    except AssertionError as error:
        if "duplicate replay write_receipt_json differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("Delete dropped replay summary regression was not caught")

    missing_key = UpsertRequest(message_type="Invoice")
    missing_key.context.tenant_id = "tenant-a"
    missing_key.context.project_id = "project-a"
    missing_key.record_json = b'{"id":"invoice-1","total_cents":100}'
    try:
        validate_replay_request("Upsert", missing_key)
    except ValueError as error:
        if "non-empty idempotency_key" not in str(error):
            raise
    else:
        raise AssertionError("missing idempotency key regression was not caught")
    spaced_key = UpsertRequest(message_type="Invoice", idempotency_key=" idem-1 ")
    spaced_key.context.tenant_id = "tenant-a"
    spaced_key.context.project_id = "project-a"
    spaced_key.record_json = b'{"id":"invoice-1","total_cents":100}'
    try:
        validate_replay_request("Upsert", spaced_key)
    except ValueError as error:
        if "idempotency_key must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("spaced Upsert idempotency_key regression was not caught")
    embedded_space_key = DeleteRequest(message_type="Invoice", idempotency_key="delete key")
    embedded_space_key.context.tenant_id = "tenant-a"
    embedded_space_key.context.project_id = "project-a"
    embedded_space_key.filter["invoice_id"] = "invoice-1"
    try:
        validate_replay_request("Delete", embedded_space_key)
    except ValueError as error:
        if "idempotency_key must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("embedded-space Delete idempotency_key regression was not caught")
    control_key = UpsertRequest(message_type="Invoice", idempotency_key="idem\0")
    control_key.context.tenant_id = "tenant-a"
    control_key.context.project_id = "project-a"
    control_key.record_json = b'{"id":"invoice-1","total_cents":100}'
    try:
        validate_replay_request("Upsert", control_key)
    except ValueError as error:
        if "idempotency_key must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character Upsert idempotency_key regression was not caught")
    missing_upsert_message_type = UpsertRequest(idempotency_key="idem-1")
    missing_upsert_message_type.context.tenant_id = "tenant-a"
    missing_upsert_message_type.context.project_id = "project-a"
    missing_upsert_message_type.record_json = b'{"id":"invoice-1","total_cents":100}'
    try:
        validate_replay_request("Upsert", missing_upsert_message_type)
    except ValueError as error:
        if "message_type" not in str(error):
            raise
    else:
        raise AssertionError("missing Upsert message_type regression was not caught")
    spaced_upsert_message_type = UpsertRequest(message_type=" Invoice ", idempotency_key="idem-1")
    spaced_upsert_message_type.context.tenant_id = "tenant-a"
    spaced_upsert_message_type.context.project_id = "project-a"
    spaced_upsert_message_type.record_json = b'{"id":"invoice-1","total_cents":100}'
    try:
        validate_replay_request("Upsert", spaced_upsert_message_type)
    except ValueError as error:
        if "message_type must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("spaced Upsert message_type regression was not caught")
    empty_upsert_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    empty_upsert_payload.context.tenant_id = "tenant-a"
    empty_upsert_payload.context.project_id = "project-a"
    try:
        validate_replay_request("Upsert", empty_upsert_payload)
    except ValueError as error:
        if "non-empty record_json" not in str(error):
            raise
    else:
        raise AssertionError("missing Upsert record_json regression was not caught")
    malformed_upsert_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    malformed_upsert_payload.context.tenant_id = "tenant-a"
    malformed_upsert_payload.context.project_id = "project-a"
    malformed_upsert_payload.record_json = b'{"id":"invoice-1"'
    try:
        validate_replay_request("Upsert", malformed_upsert_payload)
    except ValueError as error:
        if "valid JSON object" not in str(error):
            raise
    else:
        raise AssertionError("malformed Upsert record_json regression was not caught")
    array_upsert_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    array_upsert_payload.context.tenant_id = "tenant-a"
    array_upsert_payload.context.project_id = "project-a"
    array_upsert_payload.record_json = b'["invoice-1"]'
    try:
        validate_replay_request("Upsert", array_upsert_payload)
    except ValueError as error:
        if "JSON object" not in str(error):
            raise
    else:
        raise AssertionError("array Upsert record_json regression was not caught")
    empty_object_upsert_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    empty_object_upsert_payload.context.tenant_id = "tenant-a"
    empty_object_upsert_payload.context.project_id = "project-a"
    empty_object_upsert_payload.record_json = b"{}"
    try:
        validate_replay_request("Upsert", empty_object_upsert_payload)
    except ValueError as error:
        if "non-empty JSON object" not in str(error):
            raise
    else:
        raise AssertionError("empty-object Upsert record_json regression was not caught")
    duplicate_key_upsert_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    duplicate_key_upsert_payload.context.tenant_id = "tenant-a"
    duplicate_key_upsert_payload.context.project_id = "project-a"
    duplicate_key_upsert_payload.record_json = b'{"id":"invoice-1","id":"invoice-2"}'
    try:
        validate_replay_request("Upsert", duplicate_key_upsert_payload)
    except ValueError as error:
        if "record_json must not contain duplicate JSON keys" not in str(error):
            raise
    else:
        raise AssertionError("duplicate-key Upsert record_json regression was not caught")
    non_finite_upsert_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    non_finite_upsert_payload.context.tenant_id = "tenant-a"
    non_finite_upsert_payload.context.project_id = "project-a"
    non_finite_upsert_payload.record_json = b'{"id":NaN}'
    try:
        validate_replay_request("Upsert", non_finite_upsert_payload)
    except ValueError as error:
        if "record_json must not contain non-standard JSON constants" not in str(error):
            raise
    else:
        raise AssertionError("non-finite Upsert record_json regression was not caught")
    try:
        _normalize_upsert_dict({"record_json": "e30=", "record_json_object": {}})
    except ValueError as error:
        if "must use only one of record_json, record_json_object, or record_json_text" not in str(error):
            raise
    else:
        raise AssertionError("ambiguous Upsert record_json encoding regression was not caught")
    try:
        _normalize_upsert_dict({"record_json_object": ["not", "object"]})
    except ValueError as error:
        if "record_json_object must be a JSON object" not in str(error):
            raise
    else:
        raise AssertionError("non-object record_json_object regression was not caught")
    try:
        _normalize_upsert_dict({"record_json_text": {"id": "invoice-1"}})
    except ValueError as error:
        if "record_json_text must be a string" not in str(error):
            raise
    else:
        raise AssertionError("non-string record_json_text regression was not caught")
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        try:
            load_upsert(temp / "missing-upsert.json")
        except ValueError as error:
            if "proof file must exist" not in str(error):
                raise
        else:
            raise AssertionError("missing Upsert proof file regression was not caught")
        try:
            load_delete(temp / "missing-delete.json")
        except ValueError as error:
            if "proof file must exist" not in str(error):
                raise
        else:
            raise AssertionError("missing Delete proof file regression was not caught")
        oversized_upsert = temp / "oversized-upsert.json"
        oversized_upsert.write_text(" " * (MAX_PROOF_INPUT_BYTES + 1), encoding="utf-8")
        try:
            load_upsert(oversized_upsert)
        except ValueError as error:
            if "proof file must be <=" not in str(error):
                raise
        else:
            raise AssertionError("oversized Upsert proof file regression was not caught")
        try:
            load_delete(oversized_upsert)
        except ValueError as error:
            if "proof file must be <=" not in str(error):
                raise
        else:
            raise AssertionError("oversized Delete proof file regression was not caught")

        duplicate_upsert = temp / "duplicate-upsert.json"
        duplicate_upsert.write_text('{"message_type":"Invoice","message_type":"Customer"}', encoding="utf-8")
        try:
            load_upsert(duplicate_upsert)
        except ValueError as error:
            if "proof JSON must not contain duplicate key" not in str(error):
                raise
        else:
            raise AssertionError("duplicate-key Upsert proof JSON regression was not caught")

        non_finite_upsert = temp / "non-finite-upsert.json"
        non_finite_upsert.write_text('{"message_type":"Invoice","record_json_object":{"id":NaN}}', encoding="utf-8")
        try:
            load_upsert(non_finite_upsert)
        except ValueError as error:
            if "proof JSON must not contain non-standard constant NaN" not in str(error):
                raise
        else:
            raise AssertionError("non-finite Upsert proof JSON regression was not caught")

        duplicate_delete = temp / "duplicate-delete.json"
        duplicate_delete.write_text('{"message_type":"Invoice","message_type":"Customer"}', encoding="utf-8")
        try:
            load_delete(duplicate_delete)
        except ValueError as error:
            if "proof JSON must not contain duplicate key" not in str(error):
                raise
        else:
            raise AssertionError("duplicate-key Delete proof JSON regression was not caught")
    missing_delete_key = DeleteRequest(message_type="Invoice")
    missing_delete_key.context.tenant_id = "tenant-a"
    missing_delete_key.context.project_id = "project-a"
    missing_delete_key.filter["invoice_id"] = "invoice-1"
    try:
        validate_replay_request("Delete", missing_delete_key)
    except ValueError as error:
        if "non-empty idempotency_key" not in str(error):
            raise
    else:
        raise AssertionError("missing Delete idempotency key regression was not caught")
    missing_delete_message_type = DeleteRequest(idempotency_key="delete-2")
    missing_delete_message_type.context.tenant_id = "tenant-a"
    missing_delete_message_type.context.project_id = "project-a"
    missing_delete_message_type.filter["invoice_id"] = "invoice-1"
    try:
        validate_replay_request("Delete", missing_delete_message_type)
    except ValueError as error:
        if "message_type" not in str(error):
            raise
    else:
        raise AssertionError("missing Delete message_type regression was not caught")
    spaced_upsert_tenant = UpsertRequest(message_type="Invoice", idempotency_key="upsert-2")
    spaced_upsert_tenant.context.tenant_id = " tenant-a "
    spaced_upsert_tenant.context.project_id = "project-a"
    spaced_upsert_tenant.record_json = b'{"id":"invoice-1","total_cents":100}'
    try:
        validate_replay_request("Upsert", spaced_upsert_tenant)
    except ValueError as error:
        if "context.tenant_id must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("spaced Upsert tenant_id regression was not caught")
    embedded_space_delete_project = DeleteRequest(message_type="Invoice", idempotency_key="delete-2")
    embedded_space_delete_project.context.tenant_id = "tenant-a"
    embedded_space_delete_project.context.project_id = "project a"
    embedded_space_delete_project.filter["invoice_id"] = "invoice-1"
    try:
        validate_replay_request("Delete", embedded_space_delete_project)
    except ValueError as error:
        if "context.project_id must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("embedded-space Delete project_id regression was not caught")
    embedded_space_delete_message_type = DeleteRequest(message_type="Invoice Item", idempotency_key="delete-2")
    embedded_space_delete_message_type.context.tenant_id = "tenant-a"
    embedded_space_delete_message_type.context.project_id = "project-a"
    embedded_space_delete_message_type.filter["invoice_id"] = "invoice-1"
    try:
        validate_replay_request("Delete", embedded_space_delete_message_type)
    except ValueError as error:
        if "message_type must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("embedded-space Delete message_type regression was not caught")
    missing_delete_filter = DeleteRequest(message_type="Invoice", idempotency_key="delete-2")
    missing_delete_filter.context.tenant_id = "tenant-a"
    missing_delete_filter.context.project_id = "project-a"
    try:
        validate_replay_request("Delete", missing_delete_filter)
    except ValueError as error:
        if "non-empty filter" not in str(error):
            raise
    else:
        raise AssertionError("missing Delete filter regression was not caught")
    empty_delete_filter_field = DeleteRequest(message_type="Invoice", idempotency_key="delete-2")
    empty_delete_filter_field.context.tenant_id = "tenant-a"
    empty_delete_filter_field.context.project_id = "project-a"
    empty_delete_filter_field.filter[""] = "invoice-1"
    try:
        validate_replay_request("Delete", empty_delete_filter_field)
    except ValueError as error:
        if "filter field names must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("empty Delete filter field regression was not caught")
    spaced_delete_filter_field = DeleteRequest(message_type="Invoice", idempotency_key="delete-2")
    spaced_delete_filter_field.context.tenant_id = "tenant-a"
    spaced_delete_filter_field.context.project_id = "project-a"
    spaced_delete_filter_field.filter[" invoice_id "] = "invoice-1"
    try:
        validate_replay_request("Delete", spaced_delete_filter_field)
    except ValueError as error:
        if "filter field names must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("spaced Delete filter field regression was not caught")
    embedded_space_delete_filter_field = DeleteRequest(message_type="Invoice", idempotency_key="delete-2")
    embedded_space_delete_filter_field.context.tenant_id = "tenant-a"
    embedded_space_delete_filter_field.context.project_id = "project-a"
    embedded_space_delete_filter_field.filter["invoice id"] = "invoice-1"
    try:
        validate_replay_request("Delete", embedded_space_delete_filter_field)
    except ValueError as error:
        if "filter field names must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("embedded-space Delete filter field regression was not caught")
    control_delete_filter_field = DeleteRequest(message_type="Invoice", idempotency_key="delete-2")
    control_delete_filter_field.context.tenant_id = "tenant-a"
    control_delete_filter_field.context.project_id = "project-a"
    control_delete_filter_field.filter["invoice_id\0"] = "invoice-1"
    try:
        validate_replay_request("Delete", control_delete_filter_field)
    except ValueError as error:
        if "filter field names must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character Delete filter field regression was not caught")
    null_delete_filter = DeleteRequest(message_type="Invoice", idempotency_key="delete-2")
    null_delete_filter.context.tenant_id = "tenant-a"
    null_delete_filter.context.project_id = "project-a"
    null_delete_filter.filter["invoice_id"] = None
    try:
        validate_replay_request("Delete", null_delete_filter)
    except ValueError as error:
        if "filter values must not be null" not in str(error):
            raise
    else:
        raise AssertionError("null Delete filter value regression was not caught")
    mismatched_delete = DeleteRequest(message_type="Customer", idempotency_key="delete-3")
    mismatched_delete.context.tenant_id = "tenant-a"
    mismatched_delete.context.project_id = "project-a"
    mismatched_delete.filter["customer_id"] = "customer-1"
    try:
        validate_shared_replay_scope(req, mismatched_delete)
    except ValueError as error:
        if "message_type" not in str(error):
            raise
    else:
        raise AssertionError("mismatched Upsert/Delete replay scope regression was not caught")
    mismatched_delete_key = DeleteRequest(message_type="Invoice", idempotency_key="delete-3")
    mismatched_delete_key.context.tenant_id = "tenant-a"
    mismatched_delete_key.context.project_id = "project-a"
    mismatched_delete_key.filter["invoice_id"] = "invoice-1"
    try:
        validate_shared_replay_scope(req, mismatched_delete_key)
    except ValueError as error:
        if "idempotency_key" not in str(error):
            raise
    else:
        raise AssertionError("mismatched Upsert/Delete idempotency key regression was not caught")
    unrelated_delete_filter = DeleteRequest(message_type="Invoice", idempotency_key="idem-1")
    unrelated_delete_filter.context.tenant_id = "tenant-a"
    unrelated_delete_filter.context.project_id = "project-a"
    unrelated_delete_filter.filter["invoice_id"] = "invoice-404"
    try:
        validate_delete_filter_matches_upsert_payload(req, unrelated_delete_filter)
    except ValueError as error:
        if "must share at least one Delete filter field/value with Upsert record_json" not in str(error):
            raise
    else:
        raise AssertionError("mismatched Delete filter payload regression was not caught")
    try:
        _parse_headers(["authorization: Bearer one", "authorization: Bearer two"])
    except ValueError as error:
        if "duplicate gRPC metadata header" not in str(error):
            raise
    else:
        raise AssertionError("duplicate live gRPC header regression was not caught")
    try:
        _parse_headers(["Authorization: Bearer one"])
    except ValueError as error:
        if "gRPC metadata header name must contain only lowercase letters" not in str(error):
            raise
    else:
        raise AssertionError("uppercase gRPC header name regression was not caught")
    try:
        _parse_headers([" authorization: Bearer one"])
    except ValueError as error:
        if "name must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("spaced gRPC header name regression was not caught")
    try:
        _parse_headers(["authorization:  Bearer one "])
    except ValueError as error:
        if "value must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("spaced gRPC header value regression was not caught")
    try:
        _parse_headers(["Bad Header: value"])
    except ValueError as error:
        if "gRPC metadata header name must contain only lowercase letters" not in str(error):
            raise
    else:
        raise AssertionError("malformed gRPC header name regression was not caught")
    try:
        _parse_headers(["grpc-timeout: 1S"])
    except ValueError as error:
        if "must not start with grpc-" not in str(error):
            raise
    else:
        raise AssertionError("reserved gRPC header name regression was not caught")
    try:
        _parse_headers(["authorization-bin: abc"])
    except ValueError as error:
        if "binary metadata headers are not supported" not in str(error):
            raise
    else:
        raise AssertionError("binary gRPC header name regression was not caught")
    try:
        _parse_headers(["authorization: bearer\r\nx-injected: yes"])
    except ValueError as error:
        if "value must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character gRPC header value regression was not caught")
    try:
        _parse_headers(["authorization: " + ("a" * (MAX_LIVE_METADATA_VALUE_BYTES + 1))])
    except ValueError as error:
        if "value must be <=" not in str(error):
            raise
    else:
        raise AssertionError("oversized gRPC header value regression was not caught")
    try:
        _parse_headers([f"x-proof-{index}: value" for index in range(MAX_LIVE_METADATA_COUNT + 1)])
    except ValueError as error:
        if "metadata headers must be <=" not in str(error):
            raise
    else:
        raise AssertionError("excessive gRPC header count regression was not caught")
    if validate_grpc_target("127.0.0.1:50051") != "127.0.0.1:50051":
        raise AssertionError("valid gRPC target regression was not caught")
    try:
        validate_grpc_target("http://127.0.0.1:50051")
    except ValueError as error:
        if "host:port authority" not in str(error):
            raise
    else:
        raise AssertionError("URL-shaped gRPC target regression was not caught")
    try:
        validate_grpc_target("127.0.0.1:50051 ")
    except ValueError as error:
        if "surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("whitespace gRPC target regression was not caught")
    try:
        validate_grpc_target("127.0.0.1:50051\0")
    except ValueError as error:
        if "must not include control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character gRPC target regression was not caught")
    try:
        validate_grpc_target("127.0.0.1")
    except ValueError as error:
        if "include a port" not in str(error):
            raise
    else:
        raise AssertionError("missing-port gRPC target regression was not caught")
    if validate_timeout_seconds(10.0) != 10.0:
        raise AssertionError("valid timeout regression was not caught")
    if validate_timeout_seconds("10.0") != 10.0:
        raise AssertionError("canonical timeout string was rejected")
    try:
        validate_timeout_seconds(0.0)
    except ValueError as error:
        if "greater than 0" not in str(error):
            raise
    else:
        raise AssertionError("non-positive timeout regression was not caught")
    try:
        validate_timeout_seconds(" 10 ")
    except ValueError as error:
        if "surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("padded timeout regression was not caught")
    try:
        validate_timeout_seconds("1e2")
    except ValueError as error:
        if "positive decimal" not in str(error):
            raise
    else:
        raise AssertionError("non-decimal timeout regression was not caught")
    try:
        validate_timeout_seconds(float("inf"))
    except ValueError as error:
        if "finite" not in str(error):
            raise
    else:
        raise AssertionError("infinite timeout regression was not caught")
    try:
        validate_timeout_seconds(MAX_LIVE_TIMEOUT_SECONDS + 1)
    except ValueError as error:
        if "<= 120 seconds" not in str(error):
            raise
    else:
        raise AssertionError("excessive timeout regression was not caught")
    validate_complete_proof_mode(True)
    try:
        validate_complete_proof_mode(False)
    except ValueError as error:
        if "--require-all-proofs is required" not in str(error):
            raise
    else:
        raise AssertionError("missing retry-safe complete-proof flag regression was not caught")
    print("retry-safe served smoke selftest passed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run local metadata/key-gate assertions")
    parser.add_argument("--target", help="live broker gRPC target, for example 127.0.0.1:50051")
    parser.add_argument("--tls", action="store_true", help="use TLS for the gRPC channel")
    parser.add_argument("--header", action="append", default=[], help="extra gRPC metadata header as 'Name: Value'")
    parser.add_argument("--upsert-json", type=Path, help="keyed UpsertRequest JSON for served replay proof")
    parser.add_argument("--delete-json", type=Path, help="keyed DeleteRequest JSON for served replay proof")
    parser.add_argument("--require-all-proofs", action="store_true", help="require the complete Upsert+Delete retry-safe live proof")
    parser.add_argument("--timeout", default="10.0", help="per-RPC timeout in seconds")
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        return 0
    if not args.target:
        parser.error("--target is required outside --selftest")
    if not args.upsert_json:
        parser.error("--upsert-json is required outside --selftest")
    if not args.delete_json:
        parser.error("--delete-json is required outside --selftest")

    try:
        validate_complete_proof_mode(args.require_all_proofs)
        upsert_request = load_upsert(args.upsert_json, "Upsert")
        delete_request = load_delete(args.delete_json, "Delete")
        validate_replay_request("Upsert", upsert_request)
        validate_replay_request("Delete", delete_request)
        validate_shared_replay_scope(upsert_request, delete_request)
        validate_delete_filter_matches_upsert_payload(upsert_request, delete_request)
        assert_retry_metadata_gate(upsert_request, delete_request)
    except ValueError as error:
        parser.error(str(error))
    try:
        metadata = _parse_headers(args.header)
        target = validate_grpc_target(args.target)
        timeout = validate_timeout_seconds(args.timeout)
    except ValueError as error:
        parser.error(str(error))
    stub = make_stub(target, args.tls)
    check_served_replay(stub, upsert_request, metadata, timeout)
    check_served_delete_replay(stub, delete_request, metadata, timeout)
    print("retry-safe served smoke passed: generated retry metadata + served keyed Upsert/Delete replay")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
