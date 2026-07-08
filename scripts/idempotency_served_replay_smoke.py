#!/usr/bin/env python3
"""Served-path idempotency replay smoke for DataBroker Upsert/BatchUpsert.

Live mode intentionally requires operator-supplied request JSON because the
proof depends on a migrated data-plane entity table. Focused local probes may
run one proof at a time; the GitHub proof workflow passes --require-all-proofs
so a green workflow proves the complete Chapter 05 live evidence set.
--selftest covers the harness logic without a broker.
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
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
PY_GEN = ROOT / "sdk" / "python" / "gen"
if str(PY_GEN) not in sys.path:
    sys.path.insert(0, str(PY_GEN))

import grpc  # type: ignore  # noqa: E402
from google.protobuf.json_format import MessageToDict, ParseDict, ParseError  # noqa: E402
from udb.entity.v1.mutation_pb2 import MutationResponse  # noqa: E402
from udb.entity.v1.relational_pb2 import RecordSet, SelectRequest, UpsertRequest  # noqa: E402
from udb.services.v1 import data_broker_pb2  # noqa: E402
from udb.services.v1.data_broker_pb2_grpc import DataBrokerStub  # noqa: E402


FAIL_CLOSED_STATUS = "UNAVAILABLE"
UPSERT_METHOD = "/udb.services.v1.DataBroker/Upsert"
BATCH_UPSERT_METHOD = "/udb.services.v1.DataBroker/BatchUpsert"
SELECT_METHOD = "/udb.services.v1.DataBroker/Select"
MAX_LIVE_TIMEOUT_SECONDS = 120.0
MAX_PROOF_INPUT_BYTES = 1_048_576
MAX_LIVE_METADATA_COUNT = 32
MAX_LIVE_METADATA_VALUE_BYTES = 8_192
MAX_FAIL_CLOSED_ERROR_MESSAGE_BYTES = 8_192
GRPC_METADATA_NAME_CHARS = frozenset("0123456789abcdefghijklmnopqrstuvwxyz_.-")
TIMEOUT_DECIMAL_PATTERN = re.compile(r"^(?:[1-9]\d*(?:\.\d+)?|0\.\d*[1-9]\d*)$")
SUMMARY_WRITE_RECEIPT_JSON = (
    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
    '"written_at_unix_ms":1700000000000}'
)
SUMMARY_WRITE_RECEIPT_DICT = json.loads(SUMMARY_WRITE_RECEIPT_JSON)
MANIFEST_CHECKSUM_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
MUTATION_ID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
SUMMARY_MUTATION_ID = "11111111-1111-4111-8111-111111111111"
MISMATCH_MUTATION_ID = "22222222-2222-4222-8222-222222222222"

REQUIRED_LIVE_PROOF_INPUTS: tuple[tuple[str, str], ...] = (
    ("upsert_json", "keyed Upsert replay"),
    ("tenant2_upsert_json", "tenant/project key isolation"),
    ("batch_upsert_json", "BatchUpsert duplicate replay"),
    ("fail_closed_upsert_json", "dedup-store-down keyed fail-closed"),
    ("fail_closed_select_json", "dedup-store-down no-write select"),
    ("keyless_upsert_json", "dedup-store-down keyless fresh path"),
)


def _clone_request(request: UpsertRequest) -> UpsertRequest:
    clone = UpsertRequest()
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


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict:
    out: dict[str, object] = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"proof JSON must not contain duplicate key {key!r}")
        out[key] = value
    return out


def _reject_non_finite_json_constant(constant: str) -> None:
    raise ValueError(f"proof JSON must not contain non-standard constant {constant}")


def _loads_proof_json(text: str, label: str):
    try:
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except json.JSONDecodeError as error:
        raise ValueError(f"{label}: proof JSON must be valid JSON: {error.msg}") from error
    except ValueError as error:
        raise ValueError(f"{label}: {error}") from error


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
    data = _loads_proof_json(_read_proof_text(path, label), str(path))
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a JSON object")
    request = UpsertRequest()
    try:
        ParseDict(_normalize_upsert_dict(data), request)
    except ParseError as error:
        raise ValueError(f"{path}: {label} proof JSON does not match UpsertRequest: {error}") from error
    return request


def load_batch(path: Path, label: str = "BatchUpsert") -> list[UpsertRequest]:
    text = _read_proof_text(path, label).strip()
    if not text:
        raise ValueError(f"{path}: empty batch input")
    if text.startswith("["):
        rows = _loads_proof_json(text, str(path))
    else:
        rows = [
            _loads_proof_json(line, f"{path}:{line_number}")
            for line_number, line in enumerate(text.splitlines(), start=1)
            if line.strip()
        ]
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{path}: expected a JSON array or JSONL objects")
    requests: list[UpsertRequest] = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"{path}:{index}: expected a JSON object")
        request = UpsertRequest()
        try:
            ParseDict(_normalize_upsert_dict(row), request)
        except ParseError as error:
            raise ValueError(f"{path}:{index}: {label} proof JSON does not match UpsertRequest: {error}") from error
        requests.append(request)
    return requests


def load_select(path: Path, label: str = "Select") -> SelectRequest:
    data = _loads_proof_json(_read_proof_text(path, label), str(path))
    if not isinstance(data, dict):
        raise ValueError(f"{path}: {label} proof JSON must be an object")
    request = SelectRequest()
    try:
        ParseDict(data, request)
    except ParseError as error:
        raise ValueError(f"{path}: {label} proof JSON does not match SelectRequest: {error}") from error
    return request


def _proof_scope(request: UpsertRequest) -> tuple[str, str, str]:
    return (request.context.tenant_id, request.context.project_id, request.message_type)


def validate_proof_token(label: str, value: object) -> None:
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


def validate_message_type_token(label: str, value: str) -> None:
    validate_proof_token(label, value)


def validate_fail_closed_status(value: str | None) -> str:
    text = FAIL_CLOSED_STATUS if value is None else str(value)
    if not text:
        raise ValueError("dedup-store-down fail-closed proof status must be non-empty")
    if text != FAIL_CLOSED_STATUS:
        raise ValueError(f"dedup-store-down fail-closed proof must expect UNAVAILABLE, got {value!r}")
    return text


def validate_upsert_request_message(label: str, request: object) -> UpsertRequest:
    if not isinstance(request, UpsertRequest):
        raise ValueError(f"{label} runtime request must be an UpsertRequest")
    return request


def assert_databroker_method_request(method: str, request: object, expected_method: str) -> None:
    service = data_broker_pb2.DESCRIPTOR.services_by_name.get("DataBroker")
    if service is None:
        raise AssertionError("DataBroker generated service descriptor was not found")
    prefix = "/udb.services.v1.DataBroker/"
    if not method.startswith(prefix):
        raise AssertionError(f"idempotency method constant {method!r} must target {prefix}")
    method_name = method[len(prefix) :]
    if method_name != expected_method:
        raise AssertionError(
            f"idempotency method constant {method!r} names {method_name!r}, expected {expected_method!r}"
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


def validate_keyed_upsert(label: str, request: UpsertRequest) -> None:
    request = validate_upsert_request_message(label, request)
    validate_proof_token(f"{label} idempotency_key", request.idempotency_key)
    validate_proof_token(f"{label} context.tenant_id", request.context.tenant_id)
    validate_proof_token(f"{label} context.project_id", request.context.project_id)
    validate_message_type_token(f"{label} message_type", request.message_type)


def validate_upsert_payload(label: str, request: UpsertRequest) -> dict:
    if not request.record_json:
        raise ValueError(f"{label} record_json must be non-empty")
    try:
        decoded = json.loads(
            request.record_json.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} record_json must be a valid JSON object: {error}") from error
    except ValueError as error:
        if "non-standard constant" in str(error):
            raise ValueError(f"{label} record_json must not contain non-standard JSON constants: {error}") from error
        raise ValueError(f"{label} record_json must not contain duplicate JSON keys: {error}") from error
    if not isinstance(decoded, dict):
        raise ValueError(f"{label} record_json must be a JSON object")
    if not decoded:
        raise ValueError(f"{label} record_json must be a non-empty JSON object")
    return decoded


def _validate_payload_scope(label: str, payload: dict, request: UpsertRequest) -> None:
    for field_name, expected in (
        ("tenant_id", request.context.tenant_id),
        ("project_id", request.context.project_id),
    ):
        if field_name in payload and payload[field_name] != expected:
            raise ValueError(
                f"{label} record_json {field_name} must match context.{field_name}"
            )


def _shared_non_scope_payload_fields(first: dict, second: dict) -> list[str]:
    scope_fields = {"tenant_id", "project_id"}
    return [
        field
        for field, first_value in first.items()
        if field not in scope_fields and field in second and second[field] == first_value
    ]


def validate_tenant_isolation_requests(baseline: UpsertRequest, tenant2: UpsertRequest) -> dict:
    validate_keyed_upsert("keyed Upsert replay proof", baseline)
    validate_keyed_upsert("tenant/project key isolation proof", tenant2)
    baseline_payload = validate_upsert_payload("keyed Upsert replay proof", baseline)
    tenant2_payload = validate_upsert_payload("tenant/project key isolation proof", tenant2)
    _validate_payload_scope("keyed Upsert replay proof", baseline_payload, baseline)
    _validate_payload_scope("tenant/project key isolation proof", tenant2_payload, tenant2)
    if tenant2.idempotency_key != baseline.idempotency_key:
        raise ValueError("tenant/project key isolation proof must reuse the --upsert-json idempotency_key")
    if tenant2.message_type != baseline.message_type:
        raise ValueError("tenant/project key isolation proof must reuse the --upsert-json message_type")
    if tenant2.context.tenant_id == baseline.context.tenant_id:
        raise ValueError("tenant/project key isolation proof must use a different tenant_id")
    if tenant2.context.project_id == baseline.context.project_id:
        raise ValueError("tenant/project key isolation proof must use a different project_id")
    if tenant2_payload == baseline_payload:
        raise ValueError(
            "tenant/project key isolation proof must use scope-correct record_json, not an exact reused payload"
        )
    if not _shared_non_scope_payload_fields(baseline_payload, tenant2_payload):
        raise ValueError(
            "tenant/project key isolation proof must share at least one non-scope record_json field/value"
        )
    return tenant2_payload


def validate_batch_upsert_payload_pair(first_payload: dict, second_payload: dict) -> None:
    if second_payload == first_payload:
        raise ValueError(
            "BatchUpsert proof second request must carry semantically different record_json to prove first-writer replay"
        )
    shared_identity_fields = [
        field
        for field, first_value in first_payload.items()
        if (field == "id" or field.endswith("_id")) and field in second_payload and second_payload[field] == first_value
    ]
    if not shared_identity_fields:
        raise ValueError(
            "BatchUpsert proof first two requests must share at least one identity record_json field/value"
        )


def validate_batch_upsert_requests(batch: list[UpsertRequest]) -> tuple[dict, dict]:
    if not isinstance(batch, list):
        raise ValueError("BatchUpsert proof requires a list of UpsertRequest objects")
    if len(batch) != 2:
        raise ValueError(f"BatchUpsert proof requires exactly two request objects, got {len(batch)}")
    first, second = batch[0], batch[1]
    validate_keyed_upsert("BatchUpsert proof first request", first)
    validate_keyed_upsert("BatchUpsert proof second request", second)
    first_payload = validate_upsert_payload("BatchUpsert proof first request", first)
    second_payload = validate_upsert_payload("BatchUpsert proof second request", second)
    if second.idempotency_key != first.idempotency_key:
        raise ValueError("BatchUpsert proof first two requests must share idempotency_key")
    if _proof_scope(second) != _proof_scope(first):
        raise ValueError("BatchUpsert proof first two requests must share tenant_id, project_id, and message_type")
    validate_batch_upsert_payload_pair(first_payload, second_payload)
    return first_payload, second_payload


def validate_fail_closed_freshness_scope(fail_closed: UpsertRequest, keyless: UpsertRequest) -> None:
    if _proof_scope(keyless) != _proof_scope(fail_closed):
        raise ValueError(
            "keyless fail-closed freshness proof must share tenant_id, project_id, and message_type "
            "with the keyed fail-closed proof"
        )


def validate_fail_closed_freshness_payload(fail_closed: UpsertRequest, keyless: UpsertRequest) -> None:
    fail_closed_payload = validate_upsert_payload("dedup-store-down keyed fail-closed proof", fail_closed)
    keyless_payload = validate_upsert_payload("keyless fail-closed freshness proof", keyless)
    if keyless_payload != fail_closed_payload:
        raise ValueError("keyless fail-closed freshness proof must reuse the keyed fail-closed record_json")


def _identity_filter_pairs_from_payload(label: str, payload: dict) -> dict[str, object]:
    pairs: dict[str, object] = {}
    for key, value in payload.items():
        if key != "id" and not key.endswith("_id"):
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise ValueError(f"{label} identity field {key!r} must be non-empty")
            if value != stripped:
                raise ValueError(f"{label} identity field {key!r} must not include surrounding whitespace")
            if any(char.isspace() for char in stripped):
                raise ValueError(f"{label} identity field {key!r} must not include whitespace")
            pairs[key] = value
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            pairs[key] = value
        else:
            raise ValueError(f"{label} identity field {key!r} must be a scalar string or number")
    if not pairs:
        raise ValueError(f"{label} must include at least one scalar identity record_json field (id or *_id)")
    return pairs


def _select_filter_dict(request: SelectRequest) -> dict[str, object]:
    return MessageToDict(request.filter, preserving_proto_field_name=True)


def validate_fail_closed_no_write_select(fail_closed: UpsertRequest, select: SelectRequest) -> None:
    assert_databroker_method_request(SELECT_METHOD, select, "Select")
    validate_message_type_token("dedup-store-down no-write select message_type", select.message_type)
    validate_proof_token("dedup-store-down no-write select context.tenant_id", select.context.tenant_id)
    validate_proof_token("dedup-store-down no-write select context.project_id", select.context.project_id)
    if select.message_type != fail_closed.message_type:
        raise ValueError("dedup-store-down no-write select must reuse the keyed fail-closed message_type")
    if select.context.tenant_id != fail_closed.context.tenant_id:
        raise ValueError("dedup-store-down no-write select must reuse the keyed fail-closed tenant_id")
    if select.context.project_id != fail_closed.context.project_id:
        raise ValueError("dedup-store-down no-write select must reuse the keyed fail-closed project_id")
    if select.limit != 1:
        raise ValueError("dedup-store-down no-write select must set limit=1")
    payload = validate_upsert_payload("dedup-store-down keyed fail-closed proof", fail_closed)
    expected_filter = _identity_filter_pairs_from_payload("dedup-store-down no-write select", payload)
    actual_filter = _select_filter_dict(select)
    if actual_filter != expected_filter:
        raise ValueError(
            "dedup-store-down no-write select filter must exactly match keyed fail-closed identity fields"
        )


def validate_fail_closed_requests(
    fail_closed: UpsertRequest,
    no_write_select: SelectRequest | None,
    keyless: UpsertRequest | None,
    fail_closed_code: str | None = None,
) -> None:
    validate_keyed_upsert("dedup-store-down keyed fail-closed proof", fail_closed)
    validate_upsert_payload("dedup-store-down keyed fail-closed proof", fail_closed)
    validate_fail_closed_status(fail_closed_code)
    if no_write_select is not None:
        validate_fail_closed_no_write_select(fail_closed, no_write_select)
    if keyless is not None:
        keyless = validate_upsert_request_message("keyless fail-closed freshness proof", keyless)
        if keyless.idempotency_key:
            raise ValueError("keyless fail-closed freshness proof must not set idempotency_key")
        validate_proof_token("keyless fail-closed freshness proof context.tenant_id", keyless.context.tenant_id)
        validate_proof_token("keyless fail-closed freshness proof context.project_id", keyless.context.project_id)
        validate_message_type_token("keyless fail-closed freshness proof message_type", keyless.message_type)
        validate_upsert_payload("keyless fail-closed freshness proof", keyless)
        validate_fail_closed_freshness_scope(fail_closed, keyless)
        validate_fail_closed_freshness_payload(fail_closed, keyless)


def validate_live_proof_inputs(
    upsert: UpsertRequest | None,
    tenant2: UpsertRequest | None,
    batch: list[UpsertRequest] | None,
    fail_closed: UpsertRequest | None,
    no_write_select: SelectRequest | None,
    keyless: UpsertRequest | None,
    fail_closed_code: str | None = None,
) -> None:
    if upsert is not None:
        validate_keyed_upsert("keyed Upsert replay proof", upsert)
        validate_upsert_payload("keyed Upsert replay proof", upsert)
    if tenant2 is not None:
        if upsert is None:
            raise ValueError("tenant/project key isolation proof requires --upsert-json")
        validate_tenant_isolation_requests(upsert, tenant2)
    if batch is not None:
        validate_batch_upsert_requests(batch)
    if fail_closed is not None:
        validate_fail_closed_requests(fail_closed, no_write_select, keyless, fail_closed_code)
    if no_write_select is not None:
        if fail_closed is None:
            raise ValueError("dedup-store-down no-write select proof requires --fail-closed-upsert-json")
    if keyless is not None:
        if fail_closed is None:
            raise ValueError("keyless fail-closed freshness proof requires --fail-closed-upsert-json")


def validate_runtime_stub_method(label: str, stub: object, method_name: str):
    method = getattr(stub, method_name, None)
    if not callable(method):
        raise AssertionError(f"{label} runtime stub must expose callable {method_name}")
    return method


def validate_runtime_mutation_response(label: str, response: object) -> MutationResponse:
    if not isinstance(response, MutationResponse):
        raise AssertionError(f"{label} runtime response must be a MutationResponse")
    return response


def validate_runtime_record_set(label: str, response: object) -> RecordSet:
    if not isinstance(response, RecordSet):
        raise AssertionError(f"{label} runtime response must be a RecordSet")
    return response


def call_runtime_mutation(
    label: str,
    method,
    request: UpsertRequest,
    metadata,
    timeout: float,
    *,
    allow_rpc_error: bool = False,
) -> MutationResponse:
    try:
        response = method(_clone_request(request), metadata=metadata, timeout=timeout)
    except grpc.RpcError as error:
        if allow_rpc_error:
            raise
        raise AssertionError(f"{label} runtime call raised unexpected gRPC error: {error}") from error
    except Exception as error:
        raise AssertionError(f"{label} runtime call raised error: {error}") from error
    return validate_runtime_mutation_response(label, response)


def _call_upsert(
    stub,
    request: UpsertRequest,
    metadata,
    timeout: float,
    *,
    allow_rpc_error: bool = False,
) -> MutationResponse:
    assert_databroker_method_request(UPSERT_METHOD, request, "Upsert")
    upsert = validate_runtime_stub_method("Upsert replay proof", stub, "Upsert")
    return call_runtime_mutation(
        "Upsert replay proof",
        upsert,
        request,
        metadata,
        timeout,
        allow_rpc_error=allow_rpc_error,
    )


def _clone_select(request: SelectRequest) -> SelectRequest:
    clone = SelectRequest()
    clone.ParseFromString(request.SerializeToString())
    return clone


def _call_select(stub, request: SelectRequest, metadata, timeout: float) -> RecordSet:
    assert_databroker_method_request(SELECT_METHOD, request, "Select")
    select = validate_runtime_stub_method("fail-closed no-write Select proof", stub, "Select")
    try:
        response = select(_clone_select(request), metadata=metadata, timeout=timeout)
    except grpc.RpcError as error:
        raise AssertionError(f"fail-closed no-write Select proof runtime call raised unexpected gRPC error: {error}") from error
    except Exception as error:
        raise AssertionError(f"fail-closed no-write Select proof runtime call raised error: {error}") from error
    return validate_runtime_record_set("fail-closed no-write Select proof", response)


def _assert_fresh(label: str, response: MutationResponse) -> None:
    if response.was_duplicate:
        raise AssertionError(f"{label}: expected fresh response, got was_duplicate=true")
    if response.affected_rows <= 0:
        raise AssertionError(f"{label}: fresh response affected_rows must be positive, got {response.affected_rows}")


def _assert_restored_summary(
    label: str,
    first: MutationResponse,
    second: MutationResponse,
    expected_resource_authority: str | None = None,
    expected_resource_path_prefix: str | None = None,
    expected_record_json: dict | None = None,
) -> None:
    restored = False
    for field in ("record_json", "resource_uri", "write_receipt_json"):
        first_value = getattr(first, field)
        if not _summary_field_has_value(
            label,
            field,
            first_value,
            expected_resource_authority,
            expected_resource_path_prefix,
            expected_record_json,
        ):
            second_value = getattr(second, field)
            if second_value:
                raise AssertionError(f"{label}: duplicate response {field} was absent from first response")
            continue
        restored = True
        second_value = getattr(second, field)
        if second_value != first_value:
            raise AssertionError(f"{label}: duplicate response {field} differs from first response")
        if field == "write_receipt_json":
            _assert_typed_write_receipt_lockstep(label, first, first_value, "first response")
            _assert_typed_write_receipt_lockstep(label, second, second_value, "duplicate response")
            if second.write_receipt != first.write_receipt:
                raise AssertionError(f"{label}: duplicate response typed write_receipt differs from first response")
    if not restored:
        raise AssertionError(f"{label}: first response must include at least one replay summary field")


def _summary_field_has_value(
    label: str,
    field: str,
    value,
    expected_resource_authority: str | None = None,
    expected_resource_path_prefix: str | None = None,
    expected_record_json: dict | None = None,
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
            decoded = _assert_summary_json_object(label, field, stripped)
            if expected_record_json is not None:
                _assert_summary_record_json_matches_request(label, decoded, expected_record_json)
        return True
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise AssertionError(f"{label}: first response {field} must not be whitespace-only")
        if value != stripped:
            raise AssertionError(f"{label}: first response {field} must not include surrounding whitespace")
        if field == "resource_uri":
            _assert_summary_resource_uri(label, stripped, expected_resource_authority, expected_resource_path_prefix)
        if field == "write_receipt_json":
            receipt = _assert_summary_json_object(label, field, stripped)
            _assert_summary_write_receipt_json(label, receipt)
        return True
    return True


def _assert_summary_record_json_matches_request(label: str, decoded: dict, expected: dict) -> None:
    for key, value in expected.items():
        if key not in decoded or decoded[key] != value:
            raise AssertionError(f"{label}: first response record_json must include request field/value {key!r}")


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


def _resource_id_candidates_from_payload(payload: dict) -> set[str]:
    identity_candidates: set[str] = set()
    for key, value in payload.items():
        is_identity_field = key == "id" or key.endswith("_id")
        if not is_identity_field:
            continue
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
            identity_candidates.add(value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            identity_candidates.add(str(value))
        else:
            raise AssertionError("resource_uri id proof identity field value must be non-empty")
    if not identity_candidates:
        raise AssertionError("resource_uri id proof requires at least one scalar identity request field (id or *_id)")
    return identity_candidates


def _preferred_fake_resource_id(request: UpsertRequest) -> str:
    try:
        payload = validate_upsert_payload("fake Upsert", request)
    except ValueError:
        return "rec-1"
    for field_name in ("id", "record_id"):
        value = payload.get(field_name)
        if isinstance(value, str) and value.strip() == value and value:
            return value
    for field_name, value in payload.items():
        if field_name in {"tenant_id", "project_id"}:
            continue
        if field_name.endswith("_id") and isinstance(value, str) and value.strip() == value and value:
            return value
    return "rec-1"


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


def _assert_duplicate(
    label: str,
    first: MutationResponse,
    second: MutationResponse,
    expected_resource_authority: str | None = None,
    expected_resource_path_prefix: str | None = None,
    expected_resource_ids: set[str] | None = None,
    expected_record_json: dict | None = None,
) -> None:
    if not second.was_duplicate:
        raise AssertionError(f"{label}: replay did not return was_duplicate=true")
    if second.affected_rows != first.affected_rows:
        raise AssertionError(f"{label}: duplicate response affected_rows differs from first response")
    _assert_restored_summary(
        label,
        first,
        second,
        expected_resource_authority,
        expected_resource_path_prefix,
        expected_record_json,
    )
    if expected_resource_ids is not None:
        if not first.resource_uri:
            raise AssertionError(f"{label}: first response resource_uri must be present for request identity proof")
        _assert_summary_resource_uri_matches_candidates(label, first.resource_uri, expected_resource_ids)
    if not first.write_receipt_json:
        raise AssertionError(f"{label}: first response write_receipt_json must be present for write receipt proof")
    _assert_typed_write_receipt_lockstep(label, first, first.write_receipt_json, "first response")
    _assert_mutation_id(label, first, "first response")
    _assert_mutation_id(label, second, "duplicate response")
    if second.mutation_id != first.mutation_id:
        raise AssertionError(f"{label}: duplicate response mutation_id differs from first response")


def _assert_mutation_id(label: str, response: MutationResponse, response_label: str) -> None:
    mutation_id = response.mutation_id
    if not mutation_id:
        raise AssertionError(f"{label}: {response_label} mutation_id must be non-empty")
    if not MUTATION_ID_PATTERN.fullmatch(mutation_id):
        raise AssertionError(f"{label}: {response_label} mutation_id must be a lowercase UUID")


def check_replay(stub, request: UpsertRequest, metadata, timeout: float) -> None:
    try:
        validate_keyed_upsert("keyed Upsert replay proof", request)
        payload = validate_upsert_payload("keyed Upsert replay proof", request)
        runtime_metadata, runtime_timeout = validate_runtime_transport_inputs(
            "keyed Upsert replay proof",
            metadata,
            timeout,
        )
    except ValueError as error:
        raise AssertionError(str(error)) from error
    resource_id_candidates = _resource_id_candidates_from_payload(payload)
    first = _call_upsert(stub, request, runtime_metadata, runtime_timeout)
    _assert_fresh("first keyed Upsert", first)
    second = _call_upsert(stub, request, runtime_metadata, runtime_timeout)
    _assert_duplicate(
        "keyed Upsert replay",
        first,
        second,
        request.context.tenant_id,
        request.message_type,
        resource_id_candidates,
        payload,
    )


def check_tenant_isolation(
    stub,
    baseline: UpsertRequest,
    request: UpsertRequest,
    metadata,
    timeout: float,
) -> None:
    try:
        payload = validate_tenant_isolation_requests(baseline, request)
        runtime_metadata, runtime_timeout = validate_runtime_transport_inputs(
            "tenant/project key isolation proof",
            metadata,
            timeout,
        )
    except ValueError as error:
        raise AssertionError(str(error)) from error
    resource_id_candidates = _resource_id_candidates_from_payload(payload)
    first = _call_upsert(stub, request, runtime_metadata, runtime_timeout)
    _assert_fresh("same idempotency key under second tenant/project", first)
    second = _call_upsert(stub, request, runtime_metadata, runtime_timeout)
    _assert_duplicate(
        "second tenant/project keyed Upsert replay",
        first,
        second,
        request.context.tenant_id,
        request.message_type,
        resource_id_candidates,
        payload,
    )


class _CountingRequestIterator:
    def __init__(self, requests: list[UpsertRequest]) -> None:
        self._requests = requests
        self.sent_count = 0

    def __iter__(self):
        for request in self._requests:
            self.sent_count += 1
            yield _clone_request(request)


def check_batch_replay(stub, requests: list[UpsertRequest], metadata, timeout: float) -> None:
    try:
        first_payload, _second_payload = validate_batch_upsert_requests(requests)
        runtime_metadata, runtime_timeout = validate_runtime_transport_inputs(
            "BatchUpsert proof",
            metadata,
            timeout,
        )
    except ValueError as error:
        raise AssertionError(str(error)) from error
    resource_id_candidates = _resource_id_candidates_from_payload(first_payload)
    assert_databroker_method_request(BATCH_UPSERT_METHOD, requests[0], "BatchUpsert")
    batch_upsert = validate_runtime_stub_method("BatchUpsert proof", stub, "BatchUpsert")
    request_iter = _CountingRequestIterator(requests)
    responses: list[MutationResponse] = []
    try:
        response_iter = batch_upsert(
            iter(request_iter),
            metadata=runtime_metadata,
            timeout=runtime_timeout,
        )
        try:
            response_iter = iter(response_iter)
        except TypeError as error:
            raise AssertionError("BatchUpsert proof runtime response stream must be iterable") from error
        except grpc.RpcError as error:
            raise AssertionError(f"BatchUpsert proof runtime response stream iterator raised unexpected gRPC error: {error}") from error
        except Exception as error:
            raise AssertionError(f"BatchUpsert proof runtime response stream iterator could not be opened: {error}") from error
        try:
            for index, response in enumerate(response_iter, start=1):
                if index > 2:
                    raise AssertionError("BatchUpsert returned more than 2 responses, want exactly 2")
                responses.append(validate_runtime_mutation_response(f"BatchUpsert proof response {index}", response))
        except AssertionError:
            raise
        except grpc.RpcError as error:
            raise AssertionError(f"BatchUpsert proof runtime response stream raised unexpected gRPC error: {error}") from error
        except Exception as error:
            raise AssertionError(f"BatchUpsert proof runtime response stream iteration raised error: {error}") from error
    except AssertionError:
        raise
    except grpc.RpcError as error:
        raise AssertionError(f"BatchUpsert proof runtime call raised unexpected gRPC error: {error}") from error
    except Exception as error:
        raise AssertionError(f"BatchUpsert proof runtime call raised error: {error}") from error
    if request_iter.sent_count != len(requests):
        raise AssertionError(
            f"BatchUpsert proof runtime consumed {request_iter.sent_count} request objects, "
            f"want exactly {len(requests)}"
        )
    if len(responses) < 2:
        raise AssertionError(f"BatchUpsert returned {len(responses)} responses, want exactly 2")
    _assert_fresh("BatchUpsert first item", responses[0])
    _assert_fresh_request_summary("BatchUpsert first item", responses[0], requests[0])
    _assert_duplicate(
        "BatchUpsert duplicate item",
        responses[0],
        responses[1],
        requests[0].context.tenant_id,
        requests[0].message_type,
        resource_id_candidates,
        first_payload,
    )


def _assert_fresh_request_summary(label: str, response: MutationResponse, request: UpsertRequest) -> None:
    payload = validate_upsert_payload(f"{label} request", request)
    resource_id_candidates = _resource_id_candidates_from_payload(payload)
    if not response.resource_uri:
        raise AssertionError(f"{label}: fresh response resource_uri must be present for request identity proof")
    _summary_field_has_value(
        label,
        "resource_uri",
        response.resource_uri,
        request.context.tenant_id,
        request.message_type,
        payload,
    )
    _assert_summary_resource_uri_matches_candidates(label, response.resource_uri, resource_id_candidates)
    if not response.record_json:
        raise AssertionError(f"{label}: fresh response record_json must be present for request payload proof")
    _summary_field_has_value(
        label,
        "record_json",
        response.record_json,
        request.context.tenant_id,
        request.message_type,
        payload,
    )
    if not response.write_receipt_json:
        raise AssertionError(f"{label}: fresh response write_receipt_json must be present for write receipt proof")
    _summary_field_has_value(
        label,
        "write_receipt_json",
        response.write_receipt_json,
        request.context.tenant_id,
        request.message_type,
        payload,
    )
    _assert_typed_write_receipt_lockstep(
        label,
        response,
        response.write_receipt_json,
        "fresh response",
    )


def _read_rpc_status_code(label: str, error: grpc.RpcError) -> grpc.StatusCode:
    code = getattr(error, "code", None)
    if not callable(code):
        raise AssertionError(f"{label}: gRPC status code must be readable")
    try:
        status = code()
    except Exception as exc:
        raise AssertionError(f"{label}: gRPC status code could not be read: {exc}") from exc
    if not isinstance(status, grpc.StatusCode):
        raise AssertionError(f"{label}: gRPC status code must be a grpc.StatusCode")
    return status


def _assert_no_rows(label: str, response: RecordSet) -> None:
    if response.records_json:
        raise AssertionError(f"{label}: expected no records_json rows, got {len(response.records_json)}")
    if response.rows:
        raise AssertionError(f"{label}: expected no structured rows, got {len(response.rows)}")
    if response.total_count:
        raise AssertionError(f"{label}: expected total_count=0, got {response.total_count}")


def check_fail_closed(
    stub,
    keyed: UpsertRequest,
    no_write_select: SelectRequest | None,
    keyless: UpsertRequest | None,
    metadata,
    timeout: float,
    code: str,
) -> None:
    try:
        validate_fail_closed_requests(keyed, no_write_select, keyless, code)
        runtime_metadata, runtime_timeout = validate_runtime_transport_inputs(
            "dedup-store-down fail-closed proof",
            metadata,
            timeout,
        )
    except ValueError as error:
        raise AssertionError(str(error)) from error
    expected_code = validate_fail_closed_status(code)
    try:
        _call_upsert(stub, keyed, runtime_metadata, runtime_timeout, allow_rpc_error=True)
    except grpc.RpcError as error:
        actual = _read_rpc_status_code("fail-closed keyed Upsert", error).name
        if actual != expected_code:
            raise AssertionError(f"fail-closed keyed Upsert returned {actual}, want {expected_code}") from error
        _assert_rpc_error_message("fail-closed keyed Upsert", error)
    else:
        raise AssertionError("fail-closed keyed Upsert unexpectedly succeeded")

    if no_write_select is not None:
        response = _call_select(stub, no_write_select, runtime_metadata, runtime_timeout)
        _assert_no_rows("dedup-store-down no-write Select", response)

    if keyless is not None:
        response = _call_upsert(stub, keyless, runtime_metadata, runtime_timeout)
        _assert_fresh("keyless Upsert with dedup store down", response)
        _assert_fresh_request_summary("keyless Upsert with dedup store down", response, keyless)


def _assert_rpc_error_message(label: str, error: grpc.RpcError) -> None:
    details = getattr(error, "details", None)
    if not callable(details):
        raise AssertionError(f"{label}: gRPC error message must be readable")
    try:
        message = details()
    except Exception as exc:
        raise AssertionError(f"{label}: gRPC error message could not be read: {exc}") from exc
    if not isinstance(message, str):
        raise AssertionError(f"{label}: gRPC error message must be a string")
    stripped = message.strip()
    if not stripped:
        raise AssertionError(f"{label}: gRPC error message must be non-empty")
    if message != stripped:
        raise AssertionError(f"{label}: gRPC error message must not include surrounding whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in message):
        raise AssertionError(f"{label}: gRPC error message must not contain control characters")
    if len(message.encode("utf-8")) > MAX_FAIL_CLOSED_ERROR_MESSAGE_BYTES:
        raise AssertionError(f"{label}: gRPC error message must be <= {MAX_FAIL_CLOSED_ERROR_MESSAGE_BYTES} bytes")
    normalized = stripped.lower()
    if "idempotency" not in normalized or "dedup" not in normalized:
        raise AssertionError(f"{label}: gRPC error message must identify idempotency dedup")


def missing_required_live_proofs(args: argparse.Namespace) -> list[str]:
    return [label for attr, label in REQUIRED_LIVE_PROOF_INPUTS if getattr(args, attr) is None]


def _assert_validation_error(label: str, fn, needle: str) -> None:
    try:
        fn()
    except ValueError as error:
        if needle not in str(error):
            raise AssertionError(f"{label}: validation error {error!r} did not contain {needle!r}") from error
    else:
        raise AssertionError(f"{label}: live proof input validation selftest did not fail")


def make_stub(target: str, tls: bool):
    if tls:
        channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())
    else:
        channel = grpc.insecure_channel(target)
    return DataBrokerStub(channel)


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code, message: object = "idempotency dedup store unavailable"):
        super().__init__()
        self._code = code
        self._message = message

    def code(self):
        return self._code

    def details(self):
        return self._message


class _NoDetailsRpcError(grpc.RpcError):
    def __init__(self, code):
        super().__init__()
        self._code = code

    def code(self):
        return self._code


class _ThrowingDetailsRpcError(_FakeRpcError):
    def details(self):
        raise RuntimeError("details unavailable")


class _NoCodeRpcError(grpc.RpcError):
    def details(self):
        return "idempotency dedup store unavailable"


class _ThrowingCodeRpcError(_FakeRpcError):
    def code(self):
        raise RuntimeError("code unavailable")


class _NonStatusCodeRpcError(_FakeRpcError):
    def code(self):
        return "UNAVAILABLE"


class _FakeStub:
    def __init__(self) -> None:
        self.upsert_calls = 0
        self.select_response = RecordSet()

    def Upsert(self, request, metadata=None, timeout=None):
        self.upsert_calls += 1
        if request.idempotency_key == "fail":
            raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)
        resource_id = _preferred_fake_resource_id(request)
        return MutationResponse(
            mutation_id=SUMMARY_MUTATION_ID,
            resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/{resource_id}",
            record_json=request.record_json,
            affected_rows=1,
            was_duplicate=self.upsert_calls == 2,
            write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
            write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
        )

    def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
        requests = list(request_iterator)
        first_record_json = requests[0].record_json if requests else b"{}"
        for index, request in enumerate(requests):
            yield MutationResponse(
                mutation_id=SUMMARY_MUTATION_ID,
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=index > 0,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    def Select(self, request, metadata=None, timeout=None):
        return self.select_response


def run_selftest() -> None:
    stub = _FakeStub()
    req = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    req.context.tenant_id = "tenant-a"
    req.context.project_id = "project-a"
    req.record_json = b'{"id":"rec-1","total_cents":100}'
    check_replay(stub, req, (), 1.0)

    try:
        assert_databroker_method_request(UPSERT_METHOD, object(), "Upsert")
    except AssertionError as error:
        if "does not match RPC input" not in str(error):
            raise
    else:
        raise AssertionError("idempotency method/request descriptor mismatch regression was not caught")

    try:
        assert_databroker_method_request("/udb.services.v1.DataBroker/Missing", req, "Missing")
    except AssertionError as error:
        if "DataBroker generated descriptor has no method Missing" not in str(error):
            raise
    else:
        raise AssertionError("idempotency missing method descriptor regression was not caught")

    try:
        check_replay(stub, object(), (), 1.0)
    except AssertionError as error:
        if "runtime request must be an UpsertRequest" not in str(error):
            raise
    else:
        raise AssertionError("idempotency runtime request-message validation regression was not caught")

    try:
        check_replay(stub, req, [("authorization", "Bearer token")], 1.0)
    except AssertionError as error:
        if "runtime metadata must be a parsed gRPC metadata tuple" not in str(error):
            raise
    else:
        raise AssertionError("idempotency runtime metadata validation regression was not caught")

    try:
        check_replay(stub, req, (), " 1.0 ")
    except AssertionError as error:
        if "runtime timeout is invalid" not in str(error):
            raise
    else:
        raise AssertionError("idempotency runtime timeout validation regression was not caught")

    try:
        check_replay(object(), req, (), 1.0)
    except AssertionError as error:
        if "runtime stub must expose callable Upsert" not in str(error):
            raise
    else:
        raise AssertionError("idempotency runtime Upsert stub validation regression was not caught")

    class NonResponseUpsertStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            return object()

    try:
        check_replay(NonResponseUpsertStub(), req, (), 1.0)
    except AssertionError as error:
        if "runtime response must be a MutationResponse" not in str(error):
            raise
    else:
        raise AssertionError("idempotency runtime Upsert response-message validation regression was not caught")

    class FailingUpsertStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            raise RuntimeError("upsert exploded")

    try:
        check_replay(FailingUpsertStub(), req, (), 1.0)
    except AssertionError as error:
        if "runtime call raised error" not in str(error):
            raise
    else:
        raise AssertionError("idempotency runtime Upsert call-error validation regression was not caught")

    class RpcErrorUpsertStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "unexpected unavailable")

    try:
        check_replay(RpcErrorUpsertStub(), req, (), 1.0)
    except AssertionError as error:
        if "runtime call raised unexpected gRPC error" not in str(error):
            raise
    else:
        raise AssertionError("idempotency runtime Upsert unexpected-RpcError validation regression was not caught")

    runtime_keyless_replay = UpsertRequest(message_type="Invoice")
    runtime_keyless_replay.context.tenant_id = "tenant-a"
    runtime_keyless_replay.context.project_id = "project-a"
    runtime_keyless_replay.record_json = req.record_json
    try:
        check_replay(stub, runtime_keyless_replay, (), 1.0)
    except AssertionError as error:
        if "keyed Upsert replay proof idempotency_key must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("keyed Upsert runtime empty-key regression was not caught")

    class MismatchedUpsertRecordJsonStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=b'{"id":"rec-1","total_cents":999}',
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_replay(MismatchedUpsertRecordJsonStub(), req, (), 1.0)
    except AssertionError as error:
        if "keyed Upsert replay: first response record_json must include request field/value 'total_cents'" not in str(error):
            raise
    else:
        raise AssertionError("keyed Upsert record_json request binding regression was not caught")

    no_identity_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    no_identity_payload.context.tenant_id = "tenant-a"
    no_identity_payload.context.project_id = "project-a"
    no_identity_payload.record_json = b'{"total_cents":100}'
    try:
        check_replay(stub, no_identity_payload, (), 1.0)
    except AssertionError as error:
        if "resource_uri id proof requires at least one scalar identity request field" not in str(error):
            raise
    else:
        raise AssertionError("keyed Upsert missing identity field regression was not caught")

    tenant_req = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    tenant_req.context.tenant_id = "tenant-b"
    tenant_req.context.project_id = "project-b"
    tenant_req.record_json = b'{"id":"rec-1","tenant_id":"tenant-b","project_id":"project-b","total_cents":100}'
    stub.upsert_calls = 0
    check_tenant_isolation(stub, req, tenant_req, (), 1.0)

    class MismatchedTenantRecordJsonStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=b'{"id":"rec-1","tenant_id":"tenant-b","project_id":"project-b","total_cents":999}',
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_tenant_isolation(MismatchedTenantRecordJsonStub(), req, tenant_req, (), 1.0)
    except AssertionError as error:
        if "second tenant/project keyed Upsert replay: first response record_json must include request field/value 'total_cents'" not in str(error):
            raise
    else:
        raise AssertionError("tenant/project record_json request binding regression was not caught")

    class DuplicateFlagTenantFreshStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_tenant_isolation(DuplicateFlagTenantFreshStub(), req, tenant_req, (), 1.0)
    except AssertionError as error:
        if "same idempotency key under second tenant/project: expected fresh response, got was_duplicate=true" not in str(error):
            raise
    else:
        raise AssertionError("tenant/project fresh duplicate-flag regression was not caught")

    class MissingTenantReceiptReplayStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_tenant_isolation(MissingTenantReceiptReplayStub(), req, tenant_req, (), 1.0)
    except AssertionError as error:
        if "second tenant/project keyed Upsert replay: first response write_receipt_json must be present for write receipt proof" not in str(error):
            raise
    else:
        raise AssertionError("tenant/project missing write_receipt_json regression was not caught")

    class NoTenantReplayStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=b'{"id":"1"}',
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_tenant_isolation(NoTenantReplayStub(), req, tenant_req, (), 1.0)
    except AssertionError as error:
        if "second tenant/project keyed Upsert replay" not in str(error):
            raise
    else:
        raise AssertionError("tenant/project replay regression was not caught")

    try:
        check_tenant_isolation(_FakeStub(), req, req, (), 1.0)
    except AssertionError as error:
        if "tenant/project key isolation proof must use a different tenant_id" not in str(error):
            raise
    else:
        raise AssertionError("tenant/project runtime baseline regression was not caught")

    runtime_empty_key_baseline = UpsertRequest(message_type="Invoice")
    runtime_empty_key_baseline.context.tenant_id = "tenant-a"
    runtime_empty_key_baseline.context.project_id = "project-a"
    runtime_empty_key_baseline.record_json = b'{"id":"invoice-1","total_cents":100}'
    runtime_empty_key_tenant = UpsertRequest(message_type="Invoice")
    runtime_empty_key_tenant.context.tenant_id = "tenant-b"
    runtime_empty_key_tenant.context.project_id = "project-b"
    runtime_empty_key_tenant.record_json = runtime_empty_key_baseline.record_json
    try:
        check_tenant_isolation(_FakeStub(), runtime_empty_key_baseline, runtime_empty_key_tenant, (), 1.0)
    except AssertionError as error:
        if "keyed Upsert replay proof idempotency_key must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("tenant/project runtime empty-key regression was not caught")

    class BadAffectedRowsReplayStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=b'{"id":"1"}',
                affected_rows=0 if self.upsert_calls > 1 else 1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_replay(BadAffectedRowsReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "duplicate response affected_rows differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("affected_rows replay regression was not caught")

    class BadMutationIdReplayStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                mutation_id=MISMATCH_MUTATION_ID if self.upsert_calls > 1 else SUMMARY_MUTATION_ID,
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_replay(BadMutationIdReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "duplicate response mutation_id differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("mutation_id replay regression was not caught")

    class AddedMutationIdReplayStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                mutation_id=MISMATCH_MUTATION_ID if self.upsert_calls > 1 else "",
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_replay(AddedMutationIdReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response mutation_id must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("added mutation_id replay regression was not caught")

    class InvalidMutationIdReplayStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                mutation_id="MUTATION-1",
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_replay(InvalidMutationIdReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response mutation_id must be a lowercase UUID" not in str(error):
            raise
    else:
        raise AssertionError("invalid mutation_id replay regression was not caught")

    class EmptyReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(EmptyReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response must include at least one replay summary field" not in str(error):
            raise
    else:
        raise AssertionError("empty replay summary regression was not caught")

    class MissingResourceUriReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(MissingResourceUriReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri must be present for request identity proof" not in str(error):
            raise
    else:
        raise AssertionError("missing resource_uri identity proof regression was not caught")

    class MissingReceiptReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(MissingReceiptReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "keyed Upsert replay: first response write_receipt_json must be present for write receipt proof" not in str(error):
            raise
    else:
        raise AssertionError("missing write_receipt_json replay proof regression was not caught")

    class InvalidResourceUriReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(InvalidResourceUriReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri must start with udb://" not in str(error):
            raise
    else:
        raise AssertionError("invalid resource_uri replay summary regression was not caught")

    class WrongTenantResourceUriReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://other-tenant/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(WrongTenantResourceUriReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri authority must equal request tenant_id" not in str(error):
            raise
    else:
        raise AssertionError("wrong-tenant resource_uri replay summary regression was not caught")

    class WrongMessageResourceUriReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/OtherMessage/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(WrongMessageResourceUriReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri path must start with request message_type" not in str(error):
            raise
    else:
        raise AssertionError("wrong-message resource_uri replay summary regression was not caught")

    class ShortPathResourceUriReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(ShortPathResourceUriReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri path must include request message_type and resource id" not in str(error):
            raise
    else:
        raise AssertionError("short-path resource_uri replay summary regression was not caught")

    class WrongIdResourceUriReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/other-rec",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(WrongIdResourceUriReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri id must match an identity request field value" not in str(error):
            raise
    else:
        raise AssertionError("wrong-id resource_uri replay summary regression was not caught")

    class NonIdentityScalarResourceUriReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/100",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(NonIdentityScalarResourceUriReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response resource_uri id must match an identity request field value" not in str(error):
            raise
    else:
        raise AssertionError("non-identity scalar resource_uri replay summary regression was not caught")

    padded_identity_req = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    padded_identity_req.context.tenant_id = "tenant-a"
    padded_identity_req.context.project_id = "project-a"
    padded_identity_req.record_json = b'{"id":" rec-1 ","total_cents":100}'
    try:
        check_replay(_FakeStub(), padded_identity_req, (), 1.0)
    except AssertionError as error:
        if "identity field value must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("padded identity resource_uri replay summary regression was not caught")

    embedded_space_identity_req = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    embedded_space_identity_req.context.tenant_id = "tenant-a"
    embedded_space_identity_req.context.project_id = "project-a"
    embedded_space_identity_req.record_json = b'{"id":"rec 1","total_cents":100}'
    try:
        check_replay(_FakeStub(), embedded_space_identity_req, (), 1.0)
    except AssertionError as error:
        if "identity field value must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("embedded-space identity resource_uri replay summary regression was not caught")

    class WhitespaceReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                record_json=b"   ",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(WhitespaceReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must not be whitespace-only" not in str(error):
            raise
    else:
        raise AssertionError("whitespace replay summary regression was not caught")

    class MalformedRecordJsonReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                record_json=b"not-json",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(MalformedRecordJsonReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must be a valid JSON object" not in str(error):
            raise
    else:
        raise AssertionError("malformed record_json replay summary regression was not caught")

    class NonFiniteRecordJsonReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                record_json=b'{"id":NaN}',
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(NonFiniteRecordJsonReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must not contain non-standard JSON constants" not in str(error):
            raise
    else:
        raise AssertionError("non-finite record_json replay summary regression was not caught")

    class MalformedReceiptReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json="not-json",
            )

    try:
        check_replay(MalformedReceiptReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json must be a valid JSON object" not in str(error):
            raise
    else:
        raise AssertionError("malformed write_receipt_json replay summary regression was not caught")

    class MissingReceiptFieldsReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json='{"lsn":"1"}',
            )

    try:
        check_replay(MissingReceiptFieldsReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json missing fields" not in str(error):
            raise
    else:
        raise AssertionError("missing-fields write_receipt_json replay summary regression was not caught")

    class UnexpectedReceiptFieldReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1700000000000,"shadow_fence":"0/FFFF"}'
                ),
            )

    try:
        check_replay(UnexpectedReceiptFieldReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json unexpected fields ['shadow_fence']" not in str(error):
            raise
    else:
        raise AssertionError("unexpected-field write_receipt_json replay summary regression was not caught")

    class InvalidReceiptProjectionTasksReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":"task-a",'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1700000000000}'
                ),
            )

    try:
        check_replay(InvalidReceiptProjectionTasksReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json projection_task_ids must be an array" not in str(error):
            raise
    else:
        raise AssertionError("invalid projection_task_ids write_receipt_json replay summary regression was not caught")

    class DuplicateKeyRecordJsonReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                record_json=b'{"id":"1","id":"2"}',
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )

    try:
        check_replay(DuplicateKeyRecordJsonReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response record_json must not contain duplicate JSON key" not in str(error):
            raise
    else:
        raise AssertionError("duplicate-key record_json replay summary regression was not caught")

    class DuplicateKeyReceiptReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json='{"lsn":"1","lsn":"2"}',
            )

    try:
        check_replay(DuplicateKeyReceiptReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response write_receipt_json must not contain duplicate JSON key" not in str(error):
            raise
    else:
        raise AssertionError("duplicate-key write_receipt_json replay summary regression was not caught")

    class MissingTypedReceiptReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
            )

    try:
        check_replay(MissingTypedReceiptReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response typed write_receipt must be present when write_receipt_json is present" not in str(error):
            raise
    else:
        raise AssertionError("missing typed write_receipt replay summary regression was not caught")

    mismatched_write_receipt = {
        **SUMMARY_WRITE_RECEIPT_DICT,
        "manifest_checksum": "sha256:other",
    }

    class MismatchedTypedReceiptReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=mismatched_write_receipt,
            )

    try:
        check_replay(MismatchedTypedReceiptReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "first response typed write_receipt must match write_receipt_json" not in str(error):
            raise
    else:
        raise AssertionError("mismatched typed write_receipt replay summary regression was not caught")

    class AddedRecordJsonReplaySummaryStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            response = MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
            )
            if self.upsert_calls > 1:
                response.record_json = request.record_json
            return response

    try:
        check_replay(AddedRecordJsonReplaySummaryStub(), req, (), 1.0)
    except AssertionError as error:
        if "duplicate response record_json was absent from first response" not in str(error):
            raise
    else:
        raise AssertionError("added replay summary regression was not caught")

    class DroppedReceiptReplayStub(_FakeStub):
        def __init__(self) -> None:
            self.upsert_calls = 0

        def Upsert(self, request, metadata=None, timeout=None):
            self.upsert_calls += 1
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=self.upsert_calls > 1,
                write_receipt_json="" if self.upsert_calls > 1 else SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_replay(DroppedReceiptReplayStub(), req, (), 1.0)
    except AssertionError as error:
        if "duplicate response write_receipt_json differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("dropped replay summary regression was not caught")

    class ZeroAffectedRowsFreshStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=b'{"id":"1"}',
                affected_rows=0,
                was_duplicate=False,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_replay(ZeroAffectedRowsFreshStub(), req, (), 1.0)
    except AssertionError as error:
        if "fresh response affected_rows must be positive" not in str(error):
            raise
    else:
        raise AssertionError("fresh affected_rows regression was not caught")

    class DuplicateFlagFreshStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/rec-1",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_replay(DuplicateFlagFreshStub(), req, (), 1.0)
    except AssertionError as error:
        if "first keyed Upsert: expected fresh response, got was_duplicate=true" not in str(error):
            raise
    else:
        raise AssertionError("fresh duplicate-flag regression was not caught")

    batch1 = UpsertRequest(message_type="Invoice", idempotency_key="batch-1")
    batch1.context.tenant_id = "tenant-a"
    batch1.context.project_id = "project-a"
    batch2 = UpsertRequest(message_type="Invoice", idempotency_key="batch-1")
    batch2.context.tenant_id = "tenant-a"
    batch2.context.project_id = "project-a"
    batch1.record_json = b'{"id":"batch","value":1}'
    batch2.record_json = b'{"id":"batch","value":2}'
    check_batch_replay(stub, [batch1, batch2], (), 1.0)

    class IgnoringBatchRequestIteratorStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            del request_iterator
            for index, request in enumerate([batch1, batch1]):
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=batch1.record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(IgnoringBatchRequestIteratorStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert proof runtime consumed 0 request objects, want exactly 2" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert request-stream consumption regression was not caught")

    runtime_mismatch_key_batch = UpsertRequest(message_type="Invoice", idempotency_key="batch-2")
    runtime_mismatch_key_batch.context.tenant_id = "tenant-a"
    runtime_mismatch_key_batch.context.project_id = "project-a"
    runtime_mismatch_key_batch.record_json = b'{"id":"batch","value":2}'
    try:
        check_batch_replay(stub, [batch1, runtime_mismatch_key_batch], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert proof first two requests must share idempotency_key" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime input validation regression was not caught")
    try:
        check_batch_replay(stub, [batch1, object()], (), 1.0)  # type: ignore[list-item]
    except AssertionError as error:
        if "BatchUpsert proof second request runtime request must be an UpsertRequest" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime request-message validation regression was not caught")
    try:
        check_batch_replay(object(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "runtime stub must expose callable BatchUpsert" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime stub validation regression was not caught")

    class NonIterableBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            return object()

    try:
        check_batch_replay(NonIterableBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "runtime response stream must be iterable" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime response-stream validation regression was not caught")

    class ThrowingIterator:
        def __iter__(self):
            raise RuntimeError("iterator unavailable")

    class ThrowingIteratorBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            return ThrowingIterator()

    try:
        check_batch_replay(ThrowingIteratorBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "runtime response stream iterator could not be opened" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime response-stream iterator regression was not caught")

    class RpcErrorIterator:
        def __iter__(self):
            raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "iterator unavailable")

    class RpcErrorIteratorBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            return RpcErrorIterator()

    try:
        check_batch_replay(RpcErrorIteratorBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "runtime response stream iterator raised unexpected gRPC error" not in str(error):
            raise
    else:
        raise AssertionError(
            "BatchUpsert runtime response-stream iterator unexpected-RpcError regression was not caught"
        )

    class FailingStreamBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            raise RuntimeError("stream exploded")
            yield from ()

    try:
        check_batch_replay(FailingStreamBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "runtime response stream iteration raised error" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime response-stream iteration regression was not caught")

    class RpcErrorStreamBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "stream unavailable")
            yield from ()

    try:
        check_batch_replay(RpcErrorStreamBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "runtime response stream raised unexpected gRPC error" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime response-stream unexpected-RpcError regression was not caught")

    class NonResponseBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            for _request in request_iterator:
                yield object()

    try:
        check_batch_replay(NonResponseBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "runtime response must be a MutationResponse" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime response-message validation regression was not caught")

    class ExtraResponseBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index in range(3):
                request = requests[min(index, len(requests) - 1)]
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(ExtraResponseBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert returned more than 2 responses, want exactly 2" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert extra runtime response validation regression was not caught")

    class FailingBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            raise RuntimeError("batch exploded")

    try:
        check_batch_replay(FailingBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert proof runtime call raised error" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime call-error validation regression was not caught")

    class RpcErrorBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "batch unavailable")

    try:
        check_batch_replay(RpcErrorBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert proof runtime call raised unexpected gRPC error" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert runtime unexpected-RpcError validation regression was not caught")

    class BareFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                affected_rows=1,
                was_duplicate=False,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(BareFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: fresh response resource_uri must be present for request identity proof" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item summary regression was not caught")

    class NoopFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=0,
                was_duplicate=False,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=0,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(NoopFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: fresh response affected_rows must be positive" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item affected_rows regression was not caught")

    class DuplicateFlagFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(DuplicateFlagFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: expected fresh response, got was_duplicate=true" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item duplicate-flag regression was not caught")

    class MissingReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(MissingReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: fresh response write_receipt_json must be present for write receipt proof" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item receipt regression was not caught")

    class MalformedReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json="not-json",
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(MalformedReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json must be a valid JSON object" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item malformed receipt regression was not caught")

    class DuplicateKeyReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","source_lsn":"lsn-2","outbox_seq":0,'
                    '"projection_task_ids":[],"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(DuplicateKeyReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json must not contain duplicate JSON key" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item duplicate-key receipt regression was not caught")

    class MissingFieldsReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json='{"source_lsn":"lsn-1"}',
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(MissingFieldsReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json missing fields" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item missing-fields receipt regression was not caught")

    class UnexpectedFieldReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1,"shadow_fence":"0/FFFF"}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(UnexpectedFieldReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json unexpected fields ['shadow_fence']" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item unexpected-field receipt regression was not caught")

    class InvalidProjectionTasksReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":"task-a",'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(InvalidProjectionTasksReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json projection_task_ids must be an array" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item invalid projection-tasks receipt regression was not caught")

    class InvalidProjectionTaskIdReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[""],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(InvalidProjectionTaskIdReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json projection_task_ids[0] must be a non-empty unpadded string" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item invalid projection-task-id receipt regression was not caught")

    class WhitespaceProjectionTaskIdReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":["task 1"],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(WhitespaceProjectionTaskIdReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json projection_task_ids[0] must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item whitespace projection-task-id receipt regression was not caught")

    class ControlProjectionTaskIdReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":["task-1\\u0000"],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(ControlProjectionTaskIdReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json projection_task_ids[0] must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item control-character projection-task-id receipt regression was not caught")

    class InvalidTimestampReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":0}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(InvalidTimestampReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json written_at_unix_ms must be a positive integer" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item invalid timestamp receipt regression was not caught")

    class BooleanTimestampReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":true}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(BooleanTimestampReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json written_at_unix_ms must be a positive integer" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item boolean timestamp receipt regression was not caught")

    class InvalidOutboxSeqReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":-1,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(InvalidOutboxSeqReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json outbox_seq must be a non-negative integer" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item invalid outbox-seq receipt regression was not caught")

    class BooleanOutboxSeqReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":true,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(BooleanOutboxSeqReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json outbox_seq must be a non-negative integer" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item boolean outbox-seq receipt regression was not caught")

    class InvalidSourceLsnReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":1,"outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(InvalidSourceLsnReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json source_lsn must be a string" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item invalid source-lsn receipt regression was not caught")

    class EmptySourceLsnReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(EmptySourceLsnReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json source_lsn must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item empty source-lsn receipt regression was not caught")

    class PaddedSourceLsnReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":" lsn-1 ","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(PaddedSourceLsnReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json source_lsn must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item padded source-lsn receipt regression was not caught")

    class EmbeddedWhitespaceSourceLsnReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn 1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(EmbeddedWhitespaceSourceLsnReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json source_lsn must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item whitespace source-lsn receipt regression was not caught")

    class ControlSourceLsnReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1\\u0000","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(ControlSourceLsnReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json source_lsn must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item control-character source-lsn receipt regression was not caught")

    class EmptyManifestChecksumReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"","written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(EmptyManifestChecksumReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json manifest_checksum must be a non-empty string" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item empty manifest-checksum receipt regression was not caught")

    class PaddedManifestChecksumReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":" sha256:test ","written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(PaddedManifestChecksumReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json manifest_checksum must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item padded manifest-checksum receipt regression was not caught")

    class MalformedManifestChecksumReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:test","written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(MalformedManifestChecksumReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item malformed manifest-checksum receipt regression was not caught")

    class PrefixManifestChecksumReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"not-a-sha-token","written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(PrefixManifestChecksumReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item bad-prefix manifest-checksum receipt regression was not caught")

    class UppercaseManifestChecksumReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=(
                    '{"source_lsn":"lsn-1","outbox_seq":0,"projection_task_ids":[],'
                    '"manifest_checksum":"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",'
                    '"written_at_unix_ms":1}'
                ),
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(UppercaseManifestChecksumReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item uppercase manifest-checksum receipt regression was not caught")

    class MissingTypedReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(MissingTypedReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: fresh response typed write_receipt must be present when write_receipt_json is present" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item typed receipt regression was not caught")

    mismatched_first_item_write_receipt = {
        **SUMMARY_WRITE_RECEIPT_DICT,
        "manifest_checksum": "different",
    }

    class MismatchedTypedReceiptFirstBatchUpsertStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=mismatched_first_item_write_receipt,
            )
            yield MutationResponse(
                resource_uri=f"udb://{requests[0].context.tenant_id or 'tenant'}/{requests[0].message_type}/batch",
                record_json=first_record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_batch_replay(MismatchedTypedReceiptFirstBatchUpsertStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: fresh response typed write_receipt must match write_receipt_json" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert fresh item mismatched typed receipt regression was not caught")

    class NonDuplicateSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for request in requests:
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=False,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(NonDuplicateSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: replay did not return was_duplicate=true" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate flag regression was not caught")

    class MismatchedAffectedRowsSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=2 if index > 0 else 1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MismatchedAffectedRowsSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response affected_rows differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate affected_rows regression was not caught")

    class MissingMutationIdSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                if index > 0:
                    yield MutationResponse(
                        resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                        record_json=first_record_json,
                        affected_rows=1,
                        was_duplicate=True,
                        write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                        write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                    )
                    continue
                yield MutationResponse(
                    mutation_id=SUMMARY_MUTATION_ID,
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=False,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MissingMutationIdSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response mutation_id must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate missing mutation_id regression was not caught")

    class MismatchedMutationIdSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                yield MutationResponse(
                    mutation_id=MISMATCH_MUTATION_ID if index > 0 else SUMMARY_MUTATION_ID,
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MismatchedMutationIdSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response mutation_id differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate mutation_id regression was not caught")

    class AddedMutationIdSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                yield MutationResponse(
                    mutation_id=MISMATCH_MUTATION_ID if index > 0 else "",
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(AddedMutationIdSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: first response mutation_id must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate added mutation_id regression was not caught")

    class InvalidMutationIdBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                yield MutationResponse(
                    mutation_id="MUTATION-1",
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(InvalidMutationIdBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: first response mutation_id must be a lowercase UUID" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate invalid mutation_id regression was not caught")

    class MissingRecordJsonSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                if index > 0:
                    yield MutationResponse(
                        resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                        affected_rows=1,
                        was_duplicate=True,
                        write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                        write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                    )
                    continue
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=False,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MissingRecordJsonSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response record_json differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate missing record_json regression was not caught")

    class MismatchedRecordJsonSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=b'{"id":"batch","value":999}' if index > 0 else first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MismatchedRecordJsonSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response record_json differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate record_json regression was not caught")

    class MissingResourceUriSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                if index > 0:
                    yield MutationResponse(
                        record_json=first_record_json,
                        affected_rows=1,
                        was_duplicate=True,
                        write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                        write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                    )
                    continue
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=False,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MissingResourceUriSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response resource_uri differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate missing resource_uri regression was not caught")

    class MismatchedResourceUriSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                yield MutationResponse(
                    resource_uri=(
                        f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/other-batch"
                        if index > 0
                        else f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch"
                    ),
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MismatchedResourceUriSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response resource_uri differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate resource_uri regression was not caught")

    class MissingReceiptJsonSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                if index > 0:
                    yield MutationResponse(
                        resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                        record_json=first_record_json,
                        affected_rows=1,
                        was_duplicate=True,
                    )
                    continue
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=False,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MissingReceiptJsonSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response write_receipt_json differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate missing write_receipt_json regression was not caught")

    class MismatchedReceiptJsonSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=(
                        b'{"mutation_id":"other","written_at_unix_ms":1,'
                        b'"outbox_seq":1,"source_lsn":"lsn-1",'
                        b'"manifest_checksum":"sha256:1111111111111111111111111111111111111111111111111111111111111111",'
                        b'"projection_tasks":["projection-1"]}'
                        if index > 0
                        else SUMMARY_WRITE_RECEIPT_JSON
                    ),
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MismatchedReceiptJsonSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response write_receipt_json differs from first response" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate write_receipt_json regression was not caught")

    class MissingTypedReceiptSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                if index > 0:
                    yield MutationResponse(
                        resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                        record_json=first_record_json,
                        affected_rows=1,
                        was_duplicate=True,
                        write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    )
                    continue
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=False,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MissingTypedReceiptSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if (
            "BatchUpsert duplicate item: duplicate response typed write_receipt "
            "must be present when write_receipt_json is present"
        ) not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate missing typed write_receipt regression was not caught")

    class MismatchedTypedReceiptSecondBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            mismatched_receipt = {
                **SUMMARY_WRITE_RECEIPT_DICT,
                "outbox_seq": SUMMARY_WRITE_RECEIPT_DICT["outbox_seq"] + 1,
            }
            for index, request in enumerate(requests):
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=mismatched_receipt if index > 0 else SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MismatchedTypedReceiptSecondBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert duplicate item: duplicate response typed write_receipt must match write_receipt_json" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate typed write_receipt regression was not caught")

    class WrongTenantBatchReplayStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            first_record_json = requests[0].record_json if requests else b"{}"
            for index, request in enumerate(requests):
                yield MutationResponse(
                    resource_uri=f"udb://other-tenant/{request.message_type}/batch",
                    record_json=first_record_json,
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(WrongTenantBatchReplayStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response resource_uri authority must equal request tenant_id" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert resource_uri scope regression was not caught")

    class MismatchedBatchRecordJsonStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            for index, request in enumerate(requests):
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=b'{"id":"batch","value":999}',
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(MismatchedBatchRecordJsonStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response record_json must include request field/value 'value'" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert record_json request binding regression was not caught")

    class DuplicateKeyBatchRecordJsonStub(_FakeStub):
        def BatchUpsert(self, request_iterator: Iterable[UpsertRequest], metadata=None, timeout=None):
            requests = list(request_iterator)
            for index, request in enumerate(requests):
                yield MutationResponse(
                    resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/batch",
                    record_json=b'{"id":"batch","id":"other-batch","value":1}',
                    affected_rows=1,
                    was_duplicate=index > 0,
                    write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                    write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
                )

    try:
        check_batch_replay(DuplicateKeyBatchRecordJsonStub(), [batch1, batch2], (), 1.0)
    except AssertionError as error:
        if "BatchUpsert first item: first response record_json must not contain duplicate JSON key" not in str(error):
            raise
    else:
        raise AssertionError("BatchUpsert duplicate-key record_json regression was not caught")

    class FailStub(_FakeStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key:
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/fail-closed",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=False,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    fail_req = UpsertRequest(message_type="Invoice", idempotency_key="fail")
    fail_req.context.tenant_id = "tenant-a"
    fail_req.context.project_id = "project-a"
    fail_req.record_json = b'{"id":"fail-closed","total_cents":100}'
    fail_select = SelectRequest(message_type="Invoice", limit=1)
    fail_select.context.tenant_id = "tenant-a"
    fail_select.context.project_id = "project-a"
    fail_select.filter.fields["id"].string_value = "fail-closed"
    keyless = UpsertRequest(message_type="Invoice")
    keyless.context.tenant_id = "tenant-a"
    keyless.context.project_id = "project-a"
    keyless.record_json = b'{"id":"fail-closed","total_cents":100}'
    validate_live_proof_inputs(req, tenant_req, [batch1, batch2], fail_req, fail_select, keyless, FAIL_CLOSED_STATUS)
    same_tenant = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    same_tenant.context.tenant_id = "tenant-a"
    same_tenant.context.project_id = "project-b"
    same_tenant.record_json = b'{"id":"invoice-1","total_cents":100}'
    same_project = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    same_project.context.tenant_id = "tenant-b"
    same_project.context.project_id = "project-a"
    same_project.record_json = b'{"id":"invoice-1","total_cents":100}'
    stale_scope_tenant = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    stale_scope_tenant.context.tenant_id = "tenant-b"
    stale_scope_tenant.context.project_id = "project-b"
    stale_scope_tenant.record_json = (
        b'{"id":"invoice-2","tenant_id":"tenant-a","project_id":"project-a","total_cents":100}'
    )
    unrelated_payload_tenant = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    unrelated_payload_tenant.context.tenant_id = "tenant-b"
    unrelated_payload_tenant.context.project_id = "project-b"
    unrelated_payload_tenant.record_json = b'{"id":"invoice-2","total_cents":200}'
    mismatch_key_batch = UpsertRequest(message_type="Invoice", idempotency_key="batch-2")
    mismatch_key_batch.context.tenant_id = "tenant-a"
    mismatch_key_batch.context.project_id = "project-a"
    mismatch_key_batch.record_json = b'{"id":"batch","value":2}'
    same_payload_batch = UpsertRequest(message_type="Invoice", idempotency_key="batch-1")
    same_payload_batch.context.tenant_id = "tenant-a"
    same_payload_batch.context.project_id = "project-a"
    same_payload_batch.record_json = batch1.record_json
    semantically_same_payload_batch = UpsertRequest(message_type="Invoice", idempotency_key="batch-1")
    semantically_same_payload_batch.context.tenant_id = "tenant-a"
    semantically_same_payload_batch.context.project_id = "project-a"
    semantically_same_payload_batch.record_json = b'{ "value" : 1, "id" : "batch" }'
    unrelated_payload_batch = UpsertRequest(message_type="Invoice", idempotency_key="batch-1")
    unrelated_payload_batch.context.tenant_id = "tenant-a"
    unrelated_payload_batch.context.project_id = "project-a"
    unrelated_payload_batch.record_json = b'{"id":"other-batch","value":2}'
    malformed_batch_payload = UpsertRequest(message_type="Invoice", idempotency_key="batch-1")
    malformed_batch_payload.context.tenant_id = "tenant-a"
    malformed_batch_payload.context.project_id = "project-a"
    malformed_batch_payload.record_json = b"{bad-json"
    array_batch_payload = UpsertRequest(message_type="Invoice", idempotency_key="batch-1")
    array_batch_payload.context.tenant_id = "tenant-a"
    array_batch_payload.context.project_id = "project-a"
    array_batch_payload.record_json = b'["batch"]'
    extra_batch = UpsertRequest(message_type="Invoice", idempotency_key="batch-1")
    extra_batch.context.tenant_id = "tenant-a"
    extra_batch.context.project_id = "project-a"
    extra_batch.record_json = b'{"id":"batch","value":3}'
    keyed_keyless = UpsertRequest(message_type="Invoice", idempotency_key="not-keyless")
    keyed_keyless.context.tenant_id = "tenant-a"
    keyed_keyless.context.project_id = "project-a"
    whitespace_keyless = UpsertRequest(message_type="Invoice", idempotency_key=" ")
    whitespace_keyless.context.tenant_id = "tenant-a"
    whitespace_keyless.context.project_id = "project-a"
    whitespace_keyless.record_json = b'{"id":"keyless","total_cents":100}'
    unrelated_keyless = UpsertRequest(message_type="Payment")
    unrelated_keyless.context.tenant_id = "tenant-b"
    unrelated_keyless.context.project_id = "project-a"
    unrelated_keyless.record_json = b'{"id":"payment-1","total_cents":100}'
    different_payload_keyless = UpsertRequest(message_type="Invoice")
    different_payload_keyless.context.tenant_id = "tenant-a"
    different_payload_keyless.context.project_id = "project-a"
    different_payload_keyless.record_json = b'{"id":"keyless","total_cents":100}'
    empty_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    empty_payload.context.tenant_id = "tenant-a"
    empty_payload.context.project_id = "project-a"
    empty_payload.record_json = b""
    malformed_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    malformed_payload.context.tenant_id = "tenant-a"
    malformed_payload.context.project_id = "project-a"
    malformed_payload.record_json = b"not-json"
    array_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    array_payload.context.tenant_id = "tenant-a"
    array_payload.context.project_id = "project-a"
    array_payload.record_json = b'["not","an","object"]'
    empty_object_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    empty_object_payload.context.tenant_id = "tenant-a"
    empty_object_payload.context.project_id = "project-a"
    empty_object_payload.record_json = b"{}"
    missing_message_type = UpsertRequest(idempotency_key="idem-1")
    missing_message_type.context.tenant_id = "tenant-a"
    missing_message_type.context.project_id = "project-a"
    missing_message_type.record_json = b'{"id":"invoice-1","total_cents":100}'
    keyless_missing_message_type = UpsertRequest()
    keyless_missing_message_type.context.tenant_id = "tenant-a"
    keyless_missing_message_type.context.project_id = "project-a"
    keyless_missing_message_type.record_json = b'{"id":"keyless","total_cents":100}'
    spaced_message_type = UpsertRequest(message_type=" Invoice ", idempotency_key="idem-1")
    spaced_message_type.context.tenant_id = "tenant-a"
    spaced_message_type.context.project_id = "project-a"
    spaced_message_type.record_json = b'{"id":"invoice-1","total_cents":100}'
    embedded_space_message_type = UpsertRequest(message_type="Invoice Item", idempotency_key="idem-1")
    embedded_space_message_type.context.tenant_id = "tenant-a"
    embedded_space_message_type.context.project_id = "project-a"
    embedded_space_message_type.record_json = b'{"id":"invoice-1","total_cents":100}'
    spaced_idempotency_key = UpsertRequest(message_type="Invoice", idempotency_key=" idem-1 ")
    spaced_idempotency_key.context.tenant_id = "tenant-a"
    spaced_idempotency_key.context.project_id = "project-a"
    spaced_idempotency_key.record_json = b'{"id":"invoice-1","total_cents":100}'
    control_idempotency_key = UpsertRequest(message_type="Invoice", idempotency_key="idem\0")
    control_idempotency_key.context.tenant_id = "tenant-a"
    control_idempotency_key.context.project_id = "project-a"
    control_idempotency_key.record_json = b'{"id":"invoice-1","total_cents":100}'
    embedded_space_tenant = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    embedded_space_tenant.context.tenant_id = "tenant a"
    embedded_space_tenant.context.project_id = "project-a"
    embedded_space_tenant.record_json = b'{"id":"invoice-1","total_cents":100}'
    spaced_keyless_project = UpsertRequest(message_type="Invoice")
    spaced_keyless_project.context.tenant_id = "tenant-a"
    spaced_keyless_project.context.project_id = " project-a "
    spaced_keyless_project.record_json = b'{"id":"keyless","total_cents":100}'
    _assert_validation_error(
        "keyed Upsert missing message_type input",
        lambda: validate_live_proof_inputs(missing_message_type, None, None, None, None, None),
        "message_type must be non-empty",
    )
    _assert_validation_error(
        "keyless proof missing message_type input",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, None, keyless_missing_message_type),
        "message_type must be non-empty",
    )
    _assert_validation_error(
        "keyed Upsert message_type surrounding whitespace input",
        lambda: validate_live_proof_inputs(spaced_message_type, None, None, None, None, None),
        "message_type must not include surrounding whitespace",
    )
    _assert_validation_error(
        "keyed Upsert message_type embedded whitespace input",
        lambda: validate_live_proof_inputs(embedded_space_message_type, None, None, None, None, None),
        "message_type must not include whitespace",
    )
    _assert_validation_error(
        "keyed Upsert idempotency_key surrounding whitespace input",
        lambda: validate_live_proof_inputs(spaced_idempotency_key, None, None, None, None, None),
        "idempotency_key must not include surrounding whitespace",
    )
    _assert_validation_error(
        "keyed Upsert idempotency_key control-character input",
        lambda: validate_live_proof_inputs(control_idempotency_key, None, None, None, None, None),
        "idempotency_key must not contain control characters",
    )
    _assert_validation_error(
        "keyed Upsert tenant embedded whitespace input",
        lambda: validate_live_proof_inputs(embedded_space_tenant, None, None, None, None, None),
        "context.tenant_id must not include whitespace",
    )
    _assert_validation_error(
        "keyless proof project surrounding whitespace input",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, None, spaced_keyless_project),
        "context.project_id must not include surrounding whitespace",
    )
    _assert_validation_error(
        "keyed Upsert empty payload input",
        lambda: validate_live_proof_inputs(empty_payload, None, None, None, None, None),
        "record_json must be non-empty",
    )
    _assert_validation_error(
        "keyed Upsert malformed JSON payload input",
        lambda: validate_live_proof_inputs(malformed_payload, None, None, None, None, None),
        "record_json must be a valid JSON object",
    )
    _assert_validation_error(
        "keyed Upsert array JSON payload input",
        lambda: validate_live_proof_inputs(array_payload, None, None, None, None, None),
        "record_json must be a JSON object",
    )
    _assert_validation_error(
        "keyed Upsert empty JSON object input",
        lambda: validate_live_proof_inputs(empty_object_payload, None, None, None, None, None),
        "record_json must be a non-empty JSON object",
    )
    duplicate_key_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    duplicate_key_payload.context.tenant_id = "tenant-a"
    duplicate_key_payload.context.project_id = "project-a"
    duplicate_key_payload.record_json = b'{"id":"invoice-1","id":"invoice-2"}'
    _assert_validation_error(
        "keyed Upsert duplicate-key payload input",
        lambda: validate_live_proof_inputs(duplicate_key_payload, None, None, None, None, None),
        "record_json must not contain duplicate JSON keys",
    )
    non_finite_payload = UpsertRequest(message_type="Invoice", idempotency_key="idem-1")
    non_finite_payload.context.tenant_id = "tenant-a"
    non_finite_payload.context.project_id = "project-a"
    non_finite_payload.record_json = b'{"id":NaN}'
    _assert_validation_error(
        "keyed Upsert non-finite payload input",
        lambda: validate_live_proof_inputs(non_finite_payload, None, None, None, None, None),
        "record_json must not contain non-standard JSON constants",
    )
    _assert_validation_error(
        "tenant isolation same-tenant input",
        lambda: validate_live_proof_inputs(req, same_tenant, None, None, None, None),
        "different tenant_id",
    )
    _assert_validation_error(
        "tenant isolation same-project input",
        lambda: validate_live_proof_inputs(req, same_project, None, None, None, None),
        "different project_id",
    )
    _assert_validation_error(
        "tenant isolation stale-scope payload input",
        lambda: validate_live_proof_inputs(req, stale_scope_tenant, None, None, None, None),
        "record_json tenant_id must match context.tenant_id",
    )
    _assert_validation_error(
        "tenant isolation unrelated-payload input",
        lambda: validate_live_proof_inputs(req, unrelated_payload_tenant, None, None, None, None),
        "tenant/project key isolation proof must share at least one non-scope record_json field/value",
    )
    _assert_validation_error(
        "BatchUpsert mismatched duplicate key input",
        lambda: validate_live_proof_inputs(None, None, [batch1, mismatch_key_batch], None, None, None),
        "BatchUpsert proof first two requests must share idempotency_key",
    )
    _assert_validation_error(
        "BatchUpsert extra item input",
        lambda: validate_live_proof_inputs(None, None, [batch1, batch2, extra_batch], None, None, None),
        "BatchUpsert proof requires exactly two request objects",
    )
    _assert_validation_error(
        "BatchUpsert identical duplicate payload input",
        lambda: validate_live_proof_inputs(None, None, [batch1, same_payload_batch], None, None, None),
        "semantically different record_json",
    )
    _assert_validation_error(
        "BatchUpsert semantically identical duplicate payload input",
        lambda: validate_live_proof_inputs(None, None, [batch1, semantically_same_payload_batch], None, None, None),
        "semantically different record_json",
    )
    _assert_validation_error(
        "BatchUpsert unrelated duplicate payload input",
        lambda: validate_live_proof_inputs(None, None, [batch1, unrelated_payload_batch], None, None, None),
        "BatchUpsert proof first two requests must share at least one identity record_json field/value",
    )
    _assert_validation_error(
        "BatchUpsert malformed JSON payload input",
        lambda: validate_live_proof_inputs(None, None, [batch1, malformed_batch_payload], None, None, None),
        "record_json must be a valid JSON object",
    )
    _assert_validation_error(
        "BatchUpsert array JSON payload input",
        lambda: validate_live_proof_inputs(None, None, [batch1, array_batch_payload], None, None, None),
        "record_json must be a JSON object",
    )
    _assert_validation_error(
        "ambiguous record_json encoding input",
        lambda: _normalize_upsert_dict({"record_json": "e30=", "record_json_object": {}}),
        "must use only one of record_json, record_json_object, or record_json_text",
    )
    _assert_validation_error(
        "non-object record_json_object input",
        lambda: _normalize_upsert_dict({"record_json_object": ["not", "object"]}),
        "record_json_object must be a JSON object",
    )
    _assert_validation_error(
        "non-string record_json_text input",
        lambda: _normalize_upsert_dict({"record_json_text": {"id": "invoice-1"}}),
        "record_json_text must be a string",
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        _assert_validation_error(
            "missing Upsert proof file",
            lambda: load_upsert(temp / "missing-upsert.json"),
            "proof file must exist",
        )
        _assert_validation_error(
            "missing BatchUpsert proof file",
            lambda: load_batch(temp / "missing-batch.jsonl"),
            "proof file must exist",
        )
        oversized_upsert = temp / "oversized-upsert.json"
        oversized_upsert.write_text(" " * (MAX_PROOF_INPUT_BYTES + 1), encoding="utf-8")
        _assert_validation_error(
            "oversized Upsert proof file",
            lambda: load_upsert(oversized_upsert),
            "proof file must be <=",
        )
        _assert_validation_error(
            "oversized BatchUpsert proof file",
            lambda: load_batch(oversized_upsert),
            "proof file must be <=",
        )

        duplicate_upsert = temp / "duplicate-upsert.json"
        duplicate_upsert.write_text('{"message_type":"Invoice","message_type":"Customer"}', encoding="utf-8")
        _assert_validation_error(
            "duplicate-key Upsert proof JSON input",
            lambda: load_upsert(duplicate_upsert),
            "proof JSON must not contain duplicate key",
        )

        non_finite_upsert = temp / "non-finite-upsert.json"
        non_finite_upsert.write_text('{"message_type":"Invoice","record_json_object":{"id":NaN}}', encoding="utf-8")
        _assert_validation_error(
            "non-finite Upsert proof JSON input",
            lambda: load_upsert(non_finite_upsert),
            "proof JSON must not contain non-standard constant NaN",
        )

        duplicate_batch_array = temp / "duplicate-batch-array.json"
        duplicate_batch_array.write_text(
            '[{"message_type":"Invoice","message_type":"Customer"}]',
            encoding="utf-8",
        )
        _assert_validation_error(
            "duplicate-key BatchUpsert array proof JSON input",
            lambda: load_batch(duplicate_batch_array),
            "proof JSON must not contain duplicate key",
        )

        duplicate_batch_jsonl = temp / "duplicate-batch-jsonl.jsonl"
        duplicate_batch_jsonl.write_text('{"message_type":"Invoice","message_type":"Customer"}', encoding="utf-8")
        _assert_validation_error(
            "duplicate-key BatchUpsert JSONL proof input",
            lambda: load_batch(duplicate_batch_jsonl),
            "proof JSON must not contain duplicate key",
        )
    _assert_validation_error(
        "keyless proof with idempotency key input",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, None, keyed_keyless),
        "keyless fail-closed freshness proof must not set idempotency_key",
    )
    _assert_validation_error(
        "keyless proof with whitespace idempotency_key input",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, None, whitespace_keyless),
        "keyless fail-closed freshness proof must not set idempotency_key",
    )
    _assert_validation_error(
        "keyless proof with unrelated scope input",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, None, unrelated_keyless),
        "keyless fail-closed freshness proof must share tenant_id, project_id, and message_type",
    )
    _assert_validation_error(
        "keyless proof with different payload input",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, None, different_payload_keyless),
        "keyless fail-closed freshness proof must reuse the keyed fail-closed record_json",
    )
    _assert_validation_error(
        "fail-closed status weakened",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, None, keyless, "INTERNAL"),
        "must expect UNAVAILABLE",
    )
    _assert_validation_error(
        "empty fail-closed status regression was not caught",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, None, keyless, ""),
        "fail-closed proof status must be non-empty",
    )
    _assert_validation_error(
        "duplicate live gRPC header regression was not caught",
        lambda: _parse_headers(["authorization: Bearer one", "authorization: Bearer two"]),
        "duplicate gRPC metadata header",
    )
    _assert_validation_error(
        "uppercase gRPC header name regression was not caught",
        lambda: _parse_headers(["Authorization: Bearer one"]),
        "gRPC metadata header name must contain only lowercase letters",
    )
    _assert_validation_error(
        "spaced gRPC header name regression was not caught",
        lambda: _parse_headers([" authorization: Bearer one"]),
        "name must not include surrounding whitespace",
    )
    _assert_validation_error(
        "spaced gRPC header value regression was not caught",
        lambda: _parse_headers(["authorization:  Bearer one "]),
        "value must not include surrounding whitespace",
    )
    _assert_validation_error(
        "malformed gRPC header name regression was not caught",
        lambda: _parse_headers(["Bad Header: value"]),
        "gRPC metadata header name must contain only lowercase letters",
    )
    _assert_validation_error(
        "reserved gRPC header name regression was not caught",
        lambda: _parse_headers(["grpc-timeout: 1S"]),
        "must not start with grpc-",
    )
    _assert_validation_error(
        "binary gRPC header name regression was not caught",
        lambda: _parse_headers(["authorization-bin: abc"]),
        "binary metadata headers are not supported",
    )
    _assert_validation_error(
        "control-character gRPC header value regression was not caught",
        lambda: _parse_headers(["authorization: bearer\r\nx-injected: yes"]),
        "value must not contain control characters",
    )
    _assert_validation_error(
        "oversized gRPC header value regression was not caught",
        lambda: _parse_headers(["authorization: " + ("a" * (MAX_LIVE_METADATA_VALUE_BYTES + 1))]),
        "value must be <=",
    )
    _assert_validation_error(
        "excessive gRPC header count regression was not caught",
        lambda: _parse_headers([f"x-proof-{index}: value" for index in range(MAX_LIVE_METADATA_COUNT + 1)]),
        "metadata headers must be <=",
    )
    if validate_grpc_target("127.0.0.1:50051") != "127.0.0.1:50051":
        raise AssertionError("valid gRPC target regression was not caught")
    _assert_validation_error(
        "URL-shaped gRPC target regression was not caught",
        lambda: validate_grpc_target("http://127.0.0.1:50051"),
        "host:port authority",
    )
    _assert_validation_error(
        "whitespace gRPC target regression was not caught",
        lambda: validate_grpc_target("127.0.0.1:50051 "),
        "surrounding whitespace",
    )
    _assert_validation_error(
        "control-character gRPC target regression was not caught",
        lambda: validate_grpc_target("127.0.0.1:50051\0"),
        "must not include control characters",
    )
    _assert_validation_error(
        "missing-port gRPC target regression was not caught",
        lambda: validate_grpc_target("127.0.0.1"),
        "include a port",
    )
    if validate_timeout_seconds(10.0) != 10.0:
        raise AssertionError("valid timeout regression was not caught")
    if validate_timeout_seconds("10.0") != 10.0:
        raise AssertionError("canonical timeout string was rejected")
    _assert_validation_error(
        "padded timeout regression was not caught",
        lambda: validate_timeout_seconds(" 10 "),
        "must not include surrounding whitespace",
    )
    _assert_validation_error(
        "non-decimal timeout regression was not caught",
        lambda: validate_timeout_seconds("1e2"),
        "positive decimal number",
    )
    _assert_validation_error(
        "non-positive timeout regression was not caught",
        lambda: validate_timeout_seconds(0.0),
        "greater than 0",
    )
    _assert_validation_error(
        "infinite timeout regression was not caught",
        lambda: validate_timeout_seconds(float("inf")),
        "finite",
    )
    _assert_validation_error(
        "excessive timeout regression was not caught",
        lambda: validate_timeout_seconds(MAX_LIVE_TIMEOUT_SECONDS + 1),
        "<= 120 seconds",
    )
    check_fail_closed(FailStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    mismatched_fail_select = SelectRequest(message_type="Invoice", limit=1)
    mismatched_fail_select.context.tenant_id = "tenant-a"
    mismatched_fail_select.context.project_id = "project-a"
    mismatched_fail_select.filter.fields["id"].string_value = "other-row"
    _assert_validation_error(
        "fail-closed no-write select filter regression was not caught",
        lambda: validate_live_proof_inputs(None, None, None, fail_req, mismatched_fail_select, keyless),
        "filter must exactly match keyed fail-closed identity fields",
    )

    class NonEmptyNoWriteSelectStub(FailStub):
        def __init__(self) -> None:
            super().__init__()
            self.select_response = RecordSet(records_json=[b'{"id":"fail-closed"}'])

    try:
        check_fail_closed(NonEmptyNoWriteSelectStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "expected no records_json rows" not in str(error):
            raise
    else:
        raise AssertionError("fail-closed no-write Select non-empty response regression was not caught")

    try:
        check_fail_closed(FailStub(), fail_req, fail_select, keyless, (), 1.0, "")
    except AssertionError as error:
        if "fail-closed proof status must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("empty runtime fail-closed status regression was not caught")

    class MissingCodeFailClosedStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _NoCodeRpcError()
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(MissingCodeFailClosedStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC status code must be readable" not in str(error):
            raise
    else:
        raise AssertionError("missing fail-closed gRPC status-code reader regression was not caught")

    class ThrowingCodeFailClosedStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _ThrowingCodeRpcError(grpc.StatusCode.UNAVAILABLE)
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(ThrowingCodeFailClosedStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC status code could not be read" not in str(error):
            raise
    else:
        raise AssertionError("unreadable fail-closed gRPC status-code regression was not caught")

    class NonStatusCodeFailClosedStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _NonStatusCodeRpcError(grpc.StatusCode.UNAVAILABLE)
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(NonStatusCodeFailClosedStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC status code must be a grpc.StatusCode" not in str(error):
            raise
    else:
        raise AssertionError("non-StatusCode fail-closed gRPC status-code regression was not caught")

    runtime_keyless_fail_closed = UpsertRequest(message_type="Invoice")
    runtime_keyless_fail_closed.context.tenant_id = "tenant-a"
    runtime_keyless_fail_closed.context.project_id = "project-a"
    runtime_keyless_fail_closed.record_json = fail_req.record_json
    try:
        check_fail_closed(FailStub(), runtime_keyless_fail_closed, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "dedup-store-down keyed fail-closed proof idempotency_key must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("fail-closed runtime keyed-input regression was not caught")

    runtime_keyed_keyless = UpsertRequest(message_type="Invoice", idempotency_key="not-keyless")
    runtime_keyed_keyless.context.tenant_id = "tenant-a"
    runtime_keyed_keyless.context.project_id = "project-a"
    runtime_keyed_keyless.record_json = keyless.record_json
    try:
        check_fail_closed(FailStub(), fail_req, fail_select, runtime_keyed_keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "keyless fail-closed freshness proof must not set idempotency_key" not in str(error):
            raise
    else:
        raise AssertionError("fail-closed runtime keyless-input regression was not caught")

    class BareKeylessFreshnessStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)
            return MutationResponse(affected_rows=1, was_duplicate=False)

    try:
        check_fail_closed(BareKeylessFreshnessStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "fresh response resource_uri must be present for request identity proof" not in str(error):
            raise
    else:
        raise AssertionError("bare keyless fail-closed freshness regression was not caught")

    class MissingReceiptKeylessFreshnessStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/fail-closed",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=False,
            )

    try:
        check_fail_closed(MissingReceiptKeylessFreshnessStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "fresh response write_receipt_json must be present for write receipt proof" not in str(error):
            raise
    else:
        raise AssertionError("missing-receipt keyless fail-closed freshness regression was not caught")

    class DuplicateFlagKeylessFreshnessStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)
            return MutationResponse(
                resource_uri=f"udb://{request.context.tenant_id or 'tenant'}/{request.message_type}/fail-closed",
                record_json=request.record_json,
                affected_rows=1,
                was_duplicate=True,
                write_receipt_json=SUMMARY_WRITE_RECEIPT_JSON,
                write_receipt=SUMMARY_WRITE_RECEIPT_DICT,
            )

    try:
        check_fail_closed(DuplicateFlagKeylessFreshnessStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "keyless Upsert with dedup store down: expected fresh response, got was_duplicate=true" not in str(error):
            raise
    else:
        raise AssertionError("keyless fail-closed freshness duplicate-flag regression was not caught")

    class EmptyFailClosedMessageStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "")
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(EmptyFailClosedMessageStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC error message must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("empty fail-closed error message regression was not caught")

    class MissingDetailsFailClosedMessageStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _NoDetailsRpcError(grpc.StatusCode.UNAVAILABLE)
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(MissingDetailsFailClosedMessageStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC error message must be readable" not in str(error):
            raise
    else:
        raise AssertionError("missing fail-closed error message reader regression was not caught")

    class ThrowingDetailsFailClosedMessageStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _ThrowingDetailsRpcError(grpc.StatusCode.UNAVAILABLE)
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(ThrowingDetailsFailClosedMessageStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC error message could not be read" not in str(error):
            raise
    else:
        raise AssertionError("unreadable fail-closed error message regression was not caught")

    class PaddedFailClosedMessageStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, " unavailable ")
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(PaddedFailClosedMessageStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC error message must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("padded fail-closed error message regression was not caught")

    class ControlFailClosedMessageStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "unavailable\nx-injected: yes")
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(ControlFailClosedMessageStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC error message must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character fail-closed error message regression was not caught")

    class NonStringFailClosedMessageStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, 503)
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(NonStringFailClosedMessageStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC error message must be a string" not in str(error):
            raise
    else:
        raise AssertionError("non-string fail-closed error message regression was not caught")

    class OversizedFailClosedMessageStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "a" * (MAX_FAIL_CLOSED_ERROR_MESSAGE_BYTES + 1))
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(OversizedFailClosedMessageStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC error message must be <=" not in str(error):
            raise
    else:
        raise AssertionError("oversized fail-closed error message regression was not caught")

    class GenericUnavailableFailClosedMessageStub(FailStub):
        def Upsert(self, request, metadata=None, timeout=None):
            if request.idempotency_key == "fail":
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "backend unavailable")
            return super().Upsert(request, metadata=metadata, timeout=timeout)

    try:
        check_fail_closed(GenericUnavailableFailClosedMessageStub(), fail_req, fail_select, keyless, (), 1.0, FAIL_CLOSED_STATUS)
    except AssertionError as error:
        if "gRPC error message must identify idempotency dedup" not in str(error):
            raise
    else:
        raise AssertionError("generic fail-closed error message regression was not caught")
    print("idempotency served replay smoke selftest passed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run local fake-stub assertions")
    parser.add_argument("--target", help="live broker gRPC target, for example 127.0.0.1:50051")
    parser.add_argument("--tls", action="store_true", help="use TLS for the gRPC channel")
    parser.add_argument("--header", action="append", default=[], help="extra gRPC metadata header as 'Name: Value'")
    parser.add_argument(
        "--tenant2-header",
        action="append",
        default=[],
        help=(
            "tenant/project isolation metadata header as 'Name: Value'; "
            "repeat for the second tenant bearer/header set. Defaults to --header."
        ),
    )
    parser.add_argument("--upsert-json", type=Path, help="keyed UpsertRequest JSON for replay proof")
    parser.add_argument("--tenant2-upsert-json", type=Path, help="same key under another tenant/project; must not duplicate")
    parser.add_argument("--batch-upsert-json", type=Path, help="JSON array or JSONL UpsertRequest objects for BatchUpsert replay proof")
    parser.add_argument("--fail-closed-upsert-json", type=Path, help="keyed UpsertRequest JSON expected to fail when dedup table is disabled")
    parser.add_argument("--fail-closed-select-json", type=Path, help="SelectRequest JSON that must return no rows after the failed keyed Upsert")
    parser.add_argument("--keyless-upsert-json", type=Path, help="optional keyless UpsertRequest JSON expected to succeed in fail-closed mode")
    parser.add_argument("--fail-closed-code", default=FAIL_CLOSED_STATUS, help="expected gRPC code for fail-closed keyed Upsert")
    parser.add_argument("--require-all-proofs", action="store_true", help="require every Chapter 05 live proof input")
    parser.add_argument("--timeout", default="10.0", help="per-RPC timeout in seconds")
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        return 0
    if not args.target:
        parser.error("--target is required outside --selftest")
    if not any((args.upsert_json, args.tenant2_upsert_json, args.batch_upsert_json, args.fail_closed_upsert_json)):
        parser.error("at least one live proof input is required")
    if args.require_all_proofs:
        missing = missing_required_live_proofs(args)
        if missing:
            parser.error("missing required idempotency live proof inputs: " + ", ".join(missing))

    try:
        upsert = load_upsert(args.upsert_json, "keyed Upsert") if args.upsert_json else None
        tenant2 = load_upsert(args.tenant2_upsert_json, "tenant isolation Upsert") if args.tenant2_upsert_json else None
        batch = load_batch(args.batch_upsert_json, "BatchUpsert") if args.batch_upsert_json else None
        fail_closed = (
            load_upsert(args.fail_closed_upsert_json, "fail-closed Upsert") if args.fail_closed_upsert_json else None
        )
        no_write_select = (
            load_select(args.fail_closed_select_json, "fail-closed no-write Select")
            if args.fail_closed_select_json
            else None
        )
        keyless = load_upsert(args.keyless_upsert_json, "keyless Upsert") if args.keyless_upsert_json else None
        validate_live_proof_inputs(upsert, tenant2, batch, fail_closed, no_write_select, keyless, args.fail_closed_code)
    except ValueError as error:
        parser.error(str(error))

    try:
        metadata = _parse_headers(args.header)
        tenant2_metadata = _parse_headers(args.tenant2_header) if args.tenant2_header else metadata
        target = validate_grpc_target(args.target)
        timeout = validate_timeout_seconds(args.timeout)
    except ValueError as error:
        parser.error(str(error))
    stub = make_stub(target, args.tls)
    checked: list[str] = []
    if upsert is not None:
        check_replay(stub, upsert, metadata, timeout)
        checked.append("keyed Upsert replay")
    if tenant2 is not None:
        check_tenant_isolation(stub, upsert, tenant2, tenant2_metadata, timeout)
        checked.append("tenant/project key isolation")
    if batch is not None:
        check_batch_replay(stub, batch, metadata, timeout)
        checked.append("BatchUpsert replay")
    if fail_closed is not None:
        check_fail_closed(
            stub,
            fail_closed,
            no_write_select,
            keyless,
            metadata,
            timeout,
            args.fail_closed_code,
        )
        checked.append("dedup-store-down fail-closed")
    print(f"idempotency served replay smoke passed: {', '.join(checked)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
