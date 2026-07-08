#!/usr/bin/env python3
"""Served-path smoke for UDB typed error-detail trailers.

Live mode intentionally accepts operator-supplied unary gRPC request JSON. The
same harness can prove a validation error (`field_violations`) and a
quota/backpressure error (`retryable` + `retry_after_ms`) crossed the real
transport boundary under the canonical `udb-error-detail-bin` trailer. Focused
local probes may run one proof at a time; the GitHub proof workflow passes
--require-all-proofs so a green run proves the complete Chapter 14.7 live
ErrorDetail evidence set.
"""

from __future__ import annotations

import argparse
import importlib
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
from google.protobuf.json_format import ParseDict, ParseError  # noqa: E402
from google.protobuf.message import DecodeError, Message  # noqa: E402
from udb.entity.v1.error_pb2 import ErrorDetail, ErrorFieldViolation, ErrorKind  # noqa: E402
from udb.entity.v1.relational_pb2 import DeleteRequest, UpsertRequest  # noqa: E402


ERROR_DETAIL_METADATA_KEY = "udb-error-detail-bin"
VALIDATION_STATUS = "INVALID_ARGUMENT"
QUOTA_STATUS = "RESOURCE_EXHAUSTED"
MAX_LIVE_TIMEOUT_SECONDS = 120.0
MAX_PROOF_INPUT_BYTES = 1_048_576
MAX_LIVE_METADATA_COUNT = 32
MAX_LIVE_METADATA_VALUE_BYTES = 8_192
MAX_STATUS_MESSAGE_BYTES = 8_192
MAX_FIELD_VIOLATION_DESCRIPTION_BYTES = 8_192
MAX_ERROR_DETAIL_TRAILER_BYTES = 1_048_576
GRPC_METADATA_NAME_CHARS = frozenset("0123456789abcdefghijklmnopqrstuvwxyz_.-")
TIMEOUT_DECIMAL_PATTERN = re.compile(r"^(?:[1-9]\d*(?:\.\d+)?|0\.\d*[1-9]\d*)$")

REQUIRED_LIVE_PROOF_INPUTS: tuple[tuple[str, str], ...] = (
    ("validation_method", "validation method"),
    ("validation_request_module", "validation request module"),
    ("validation_request_message", "validation request message"),
    ("validation_request_json", "validation request JSON"),
    ("validation_field", "validation field violation"),
    ("quota_method", "quota method"),
    ("quota_request_module", "quota request module"),
    ("quota_request_message", "quota request message"),
    ("quota_request_json", "quota request JSON"),
    ("quota_retry_after_min_ms", "quota retry-after floor"),
    ("quota_backend", "quota ErrorDetail backend"),
    ("quota_operation", "quota ErrorDetail operation"),
)


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
    return any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)


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


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict:
    out: dict[str, object] = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"request JSON must not contain duplicate key {key!r}")
        out[key] = value
    return out


def _reject_non_finite_json_constant(constant: str) -> None:
    raise ValueError(f"request JSON must not contain non-standard constant {constant}")


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


def validate_request_module_name(module_name: str, label: str) -> str:
    if module_name != module_name.strip():
        raise ValueError(f"{label} request module must not include surrounding whitespace")
    if not module_name:
        raise ValueError(f"{label} request module must be non-empty")
    if any(ch.isspace() for ch in module_name):
        raise ValueError(f"{label} request module must not include whitespace")
    if any(not part or not part.isidentifier() for part in module_name.split(".")):
        raise ValueError(f"{label} request module must be a dotted Python module path")
    return module_name


def validate_request_message_name(message_name: str, label: str) -> str:
    if message_name != message_name.strip():
        raise ValueError(f"{label} request message must not include surrounding whitespace")
    if not message_name:
        raise ValueError(f"{label} request message must be non-empty")
    if not message_name.isidentifier():
        raise ValueError(f"{label} request message must be a Python identifier")
    return message_name


def load_request(module_name: str, message_name: str, path: Path, label: str = "request"):
    module_name = validate_request_module_name(module_name, label)
    message_name = validate_request_message_name(message_name, label)
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise ValueError(f"{label} request module {module_name!r} could not be imported: {error}") from error
    try:
        message_type = getattr(module, message_name)
    except AttributeError as error:
        raise ValueError(f"{label} request module {module_name!r} does not expose {message_name!r}") from error
    if not callable(message_type):
        raise ValueError(f"{label} request module {module_name!r} does not expose protobuf message class {message_name!r}")
    try:
        request = message_type()
    except Exception as error:
        raise ValueError(f"{label} request message {module_name}.{message_name} could not be constructed: {error}") from error
    if not isinstance(request, Message):
        raise ValueError(f"{label} request message {module_name}.{message_name} is not a protobuf message")
    try:
        data = json.loads(
            _read_proof_text(path, label),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except json.JSONDecodeError as error:
        raise ValueError(f"{path}: request JSON must be a valid JSON object: {error.msg}") from error
    except ValueError as error:
        raise ValueError(f"{path}: {error}") from error
    if not isinstance(data, dict):
        raise ValueError(f"{path}: request JSON must be a JSON object")
    try:
        ParseDict(data, request)
    except ParseError as error:
        raise ValueError(f"{path}: request JSON does not match {module_name}.{message_name}: {error}") from error
    return request


def _snake_case_token(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def _method_service_parts(method: str) -> tuple[str, str, str]:
    validate_method_path("served proof", method)
    _, service_path, method_name = method.split("/")
    package, service_name = service_path.rsplit(".", 1)
    return package, service_name, method_name


def _service_descriptor_module_candidates(method: str, request: Message) -> tuple[str, ...]:
    package, service_name, _method_name = _method_service_parts(method)
    package_module = package
    request_module = f"{request.DESCRIPTOR.file.name.removesuffix('.proto').replace('/', '.')}_pb2"
    service_snake = _snake_case_token(service_name)
    candidates = [
        request_module,
        f"{package_module}.{service_snake}_pb2",
        f"{package_module}.{service_snake.removesuffix('_service')}_service_pb2",
    ]
    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            out.append(candidate)
    return tuple(out)


def assert_request_matches_method(method: str, request: Message) -> None:
    package, service_name, method_name = _method_service_parts(method)
    expected_input = None
    load_errors: list[str] = []
    for module_name in _service_descriptor_module_candidates(method, request):
        try:
            module = importlib.import_module(module_name)
        except ImportError as error:
            load_errors.append(f"{module_name}: {error}")
            continue
        service = getattr(module, "DESCRIPTOR", None)
        service = getattr(service, "services_by_name", {}).get(service_name) if service is not None else None
        if service is None:
            continue
        method_descriptor = service.methods_by_name.get(method_name)
        if method_descriptor is None:
            raise AssertionError(f"{method}: generated service descriptor has no method {method_name!r}")
        expected_input = method_descriptor.input_type.full_name
        break
    if expected_input is None:
        searched = ", ".join(_service_descriptor_module_candidates(method, request))
        suffix = f"; import errors: {'; '.join(load_errors)}" if load_errors else ""
        raise AssertionError(
            f"{method}: generated service descriptor was not found in candidate modules {searched}{suffix}"
        )
    actual_input = request.DESCRIPTOR.full_name
    if actual_input != expected_input:
        raise AssertionError(
            f"{method}: request message {actual_input!r} does not match RPC input {expected_input!r}"
        )


def _trailing_metadata_items(error: grpc.RpcError) -> Iterable[tuple[object, object]]:
    getter = getattr(error, "trailing_metadata", None)
    if getter is None:
        return
    try:
        metadata = getter()
    except Exception as error:
        raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer metadata could not be read: {error}") from error
    if not metadata:
        return
    try:
        iterator = iter(metadata)
    except TypeError as error:
        raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer metadata must be iterable") from error
    while True:
        try:
            item = next(iterator)
        except StopIteration:
            break
        except Exception as error:
            raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer metadata iteration failed: {error}") from error
        try:
            key, value = item
        except (TypeError, ValueError) as error:
            raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer metadata item must be a key/value pair") from error
        except Exception as error:
            raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer metadata item could not be read: {error}") from error
        yield key, value


def decode_error_detail(error: grpc.RpcError) -> ErrorDetail:
    matches: list[object] = []
    for key, value in _trailing_metadata_items(error):
        if not isinstance(key, str):
            raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer metadata key must be a string")
        if key != key.lower():
            raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer metadata key must be lowercase")
        if key != ERROR_DETAIL_METADATA_KEY:
            continue
        matches.append(value)
    if not matches:
        raise AssertionError(f"missing {ERROR_DETAIL_METADATA_KEY} trailer")
    if len(matches) > 1:
        raise AssertionError(f"expected exactly one {ERROR_DETAIL_METADATA_KEY} trailer, got {len(matches)}")
    value = matches[0]
    if not isinstance(value, (bytes, bytearray)):
        raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer must be bytes, got {type(value).__name__}")
    if len(value) > MAX_ERROR_DETAIL_TRAILER_BYTES:
        raise AssertionError(f"{ERROR_DETAIL_METADATA_KEY} trailer must be <= {MAX_ERROR_DETAIL_TRAILER_BYTES} bytes")
    detail = ErrorDetail()
    try:
        detail.ParseFromString(bytes(value))
    except DecodeError as error:
        raise AssertionError(f"invalid {ERROR_DETAIL_METADATA_KEY} trailer: {error}") from error
    return detail


def rpc_status_message(error: grpc.RpcError) -> str:
    details = getattr(error, "details", None)
    if not callable(details):
        return ""
    try:
        value = details()
    except Exception as error:
        raise AssertionError(f"gRPC status message could not be read: {error}") from error
    if value is None:
        return ""
    if not isinstance(value, str):
        raise AssertionError(f"gRPC status message must be a string, got {type(value).__name__}")
    return value


def check_error_detail(
    label: str,
    error: grpc.RpcError,
    expected_status: str,
    expected_kind: str,
    expected_retryable: bool | None = None,
    min_retry_after_ms: int | None = None,
    expected_field: str | None = None,
    expected_backend: str | None = None,
    expected_operation: str | None = None,
) -> ErrorDetail:
    try:
        code = error.code()
    except Exception as error:
        raise AssertionError(f"{label}: gRPC status code could not be read: {error}") from error
    if code is None:
        actual_status = ""
    elif isinstance(code, grpc.StatusCode):
        actual_status = code.name
    else:
        raise AssertionError(f"{label}: gRPC status code must be a grpc.StatusCode, got {type(code).__name__}")
    if actual_status != expected_status:
        raise AssertionError(f"{label}: got gRPC {actual_status}, want {expected_status}")
    message = rpc_status_message(error)
    stripped_message = message.strip()
    if not stripped_message:
        raise AssertionError(f"{label}: gRPC status message must be non-empty")
    if message != stripped_message:
        raise AssertionError(f"{label}: gRPC status message must not include surrounding whitespace")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in message):
        raise AssertionError(f"{label}: gRPC status message must not contain control characters")
    if len(message.encode("utf-8")) > MAX_STATUS_MESSAGE_BYTES:
        raise AssertionError(f"{label}: gRPC status message must be <= {MAX_STATUS_MESSAGE_BYTES} bytes")

    detail = decode_error_detail(error)
    try:
        actual_kind = ErrorKind.Name(detail.kind)
    except ValueError as error:
        raise AssertionError(f"{label}: got unknown ErrorDetail.kind {detail.kind}") from error
    if actual_kind != expected_kind:
        raise AssertionError(f"{label}: got ErrorDetail.kind {actual_kind}, want {expected_kind}")
    if expected_backend is not None:
        _assert_error_detail_token(label, "backend", detail.backend)
        if detail.backend != expected_backend:
            raise AssertionError(f"{label}: got ErrorDetail.backend {detail.backend!r}, want {expected_backend!r}")
    if expected_operation is not None:
        _assert_error_detail_token(label, "operation", detail.operation)
        if detail.operation != expected_operation:
            raise AssertionError(f"{label}: got ErrorDetail.operation {detail.operation!r}, want {expected_operation!r}")
    if expected_retryable is not None and detail.retryable != expected_retryable:
        raise AssertionError(f"{label}: got retryable={detail.retryable}, want {expected_retryable}")
    if min_retry_after_ms is not None and detail.retry_after_ms < min_retry_after_ms:
        raise AssertionError(
            f"{label}: got retry_after_ms={detail.retry_after_ms}, want >= {min_retry_after_ms}"
        )
    if min_retry_after_ms is not None and detail.field_violations:
        fields = [violation.field for violation in detail.field_violations]
        raise AssertionError(f"{label}: quota/backpressure detail must not include field_violations: {fields!r}")
    if expected_field and detail.retry_after_ms != 0:
        raise AssertionError(f"{label}: validation detail must not include retry_after_ms={detail.retry_after_ms}")
    if expected_field and (detail.backend or detail.operation):
        raise AssertionError(
            f"{label}: validation detail must not include backend/operation "
            f"{detail.backend!r}/{detail.operation!r}"
        )
    for index, violation in enumerate(detail.field_violations):
        field = violation.field
        stripped_field = field.strip()
        if not stripped_field:
            raise AssertionError(f"{label}: field_violations[{index}].field must be non-empty")
        if field != stripped_field:
            raise AssertionError(f"{label}: field_violations[{index}].field must not include surrounding whitespace")
        if any(char.isspace() for char in stripped_field):
            raise AssertionError(f"{label}: field_violations[{index}].field must not include whitespace")
        if _contains_control_character(stripped_field):
            raise AssertionError(f"{label}: field_violations[{index}].field must not include control characters")
        description = violation.description
        stripped_description = description.strip()
        if not stripped_description:
            raise AssertionError(
                f"{label}: field violation {violation.field!r} must include a non-empty description"
            )
        if description != stripped_description:
            raise AssertionError(
                f"{label}: field violation {violation.field!r} description must not include surrounding whitespace"
            )
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in description):
            raise AssertionError(
                f"{label}: field violation {violation.field!r} description must not contain control characters"
            )
        if len(description.encode("utf-8")) > MAX_FIELD_VIOLATION_DESCRIPTION_BYTES:
            raise AssertionError(
                f"{label}: field violation {violation.field!r} description must be "
                f"<= {MAX_FIELD_VIOLATION_DESCRIPTION_BYTES} bytes"
            )
    if expected_field:
        matching = [violation for violation in detail.field_violations if violation.field == expected_field]
        if not matching:
            fields = [violation.field for violation in detail.field_violations]
            raise AssertionError(f"{label}: field_violations {fields!r} do not include {expected_field!r}")
        if len(detail.field_violations) != 1:
            fields = [violation.field for violation in detail.field_violations]
            raise AssertionError(
                f"{label}: validation proof must include exactly {expected_field!r}, got field_violations {fields!r}"
            )
    return detail


def _assert_error_detail_token(label: str, field: str, value: str) -> None:
    stripped = value.strip()
    if not stripped:
        raise AssertionError(f"{label}: ErrorDetail.{field} must be non-empty")
    if value != stripped:
        raise AssertionError(f"{label}: ErrorDetail.{field} must not include surrounding whitespace")
    if any(char.isspace() for char in stripped):
        raise AssertionError(f"{label}: ErrorDetail.{field} must not include whitespace")
    if _contains_control_character(stripped):
        raise AssertionError(f"{label}: ErrorDetail.{field} must not include control characters")


def make_channel(target: str, tls: bool):
    if tls:
        return grpc.secure_channel(target, grpc.ssl_channel_credentials())
    return grpc.insecure_channel(target)


def validate_runtime_channel_method(label: str, channel: object):
    method = getattr(channel, "unary_unary", None)
    if not callable(method):
        raise ValueError(f"{label} runtime channel must expose callable unary_unary")
    return method


def validate_runtime_unary_call(method: str, call: object):
    if not callable(call):
        raise AssertionError(f"{method}: runtime unary call must be callable")
    return call


def invoke_expect_error(unary_unary, method: str, request, metadata, timeout: float) -> grpc.RpcError:
    try:
        call = unary_unary(
            method,
            request_serializer=request.SerializeToString,
            response_deserializer=lambda data: data,
        )
    except Exception as error:
        raise AssertionError(f"{method}: runtime unary factory raised error: {error}") from error
    call = validate_runtime_unary_call(method, call)
    try:
        call(request, metadata=metadata, timeout=timeout)
    except grpc.RpcError as error:
        return error
    except Exception as error:
        raise AssertionError(f"{method}: runtime unary call raised non-gRPC error: {error}") from error
    raise AssertionError(f"{method}: expected gRPC error, got success")


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


def run_live_check(
    label: str,
    channel,
    method: str,
    request,
    metadata,
    timeout: float,
    expected_status: str,
    expected_kind: str,
    expected_retryable: bool | None,
    min_retry_after_ms: int | None,
    expected_field: str | None,
    expected_backend: str | None = None,
    expected_operation: str | None = None,
) -> None:
    try:
        validate_method_path(f"{label} runtime proof", method)
        validate_live_check_expectations(
            label,
            expected_status,
            expected_kind,
            expected_retryable,
            min_retry_after_ms,
            expected_field,
            expected_backend,
            expected_operation,
        )
        validate_runtime_request_message(label, request)
        assert_request_matches_method(method, request)
        validated_metadata = validate_runtime_metadata(label, metadata)
        validated_timeout = validate_runtime_timeout_seconds(label, timeout)
        unary_unary = validate_runtime_channel_method(label, channel)
    except ValueError as error:
        raise AssertionError(str(error)) from error
    error = invoke_expect_error(unary_unary, method, request, validated_metadata, validated_timeout)
    check_error_detail(
        label,
        error,
        expected_status,
        expected_kind,
        expected_retryable,
        min_retry_after_ms,
        expected_field,
        expected_backend,
        expected_operation,
    )


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code, detail: ErrorDetail, message: object = "typed error detail"):
        super().__init__()
        self._code = code
        self._detail = detail
        self._message = message

    def code(self):
        return self._code

    def details(self):
        return self._message

    def trailing_metadata(self):
        return ((ERROR_DETAIL_METADATA_KEY, self._detail.SerializeToString()),)


class _DuplicateTrailerRpcError(_FakeRpcError):
    def trailing_metadata(self):
        encoded = self._detail.SerializeToString()
        return (
            (ERROR_DETAIL_METADATA_KEY, encoded),
            (ERROR_DETAIL_METADATA_KEY, encoded),
        )


class _NonStringTrailerKeyRpcError(_FakeRpcError):
    def trailing_metadata(self):
        return ((object(), self._detail.SerializeToString()),)


class _UppercaseTrailerKeyRpcError(_FakeRpcError):
    def trailing_metadata(self):
        return ((ERROR_DETAIL_METADATA_KEY.upper(), self._detail.SerializeToString()),)


class _MalformedTrailerMetadataItemRpcError(_FakeRpcError):
    def trailing_metadata(self):
        return ((ERROR_DETAIL_METADATA_KEY, self._detail.SerializeToString(), "extra"),)


class _FailingTrailerMetadataItem:
    def __iter__(self):
        raise RuntimeError("metadata item unavailable")


class _FailingTrailerMetadataItemRpcError(_FakeRpcError):
    def trailing_metadata(self):
        return (_FailingTrailerMetadataItem(),)


class _NonIterableTrailerMetadataRpcError(_FakeRpcError):
    def trailing_metadata(self):
        return 1


class _FailingTrailerMetadataIterator:
    def __iter__(self):
        return self

    def __next__(self):
        raise RuntimeError("metadata iterator unavailable")


class _FailingTrailerMetadataIteratorRpcError(_FakeRpcError):
    def trailing_metadata(self):
        return _FailingTrailerMetadataIterator()


class _ThrowingTrailerMetadataRpcError(_FakeRpcError):
    def trailing_metadata(self):
        raise RuntimeError("trailers unavailable")


class _ThrowingDetailsRpcError(_FakeRpcError):
    def details(self):
        raise RuntimeError("details unavailable")


class _ThrowingCodeRpcError(_FakeRpcError):
    def code(self):
        raise RuntimeError("code unavailable")


class _MalformedTrailerRpcError(grpc.RpcError):
    def __init__(self, code):
        super().__init__()
        self._code = code

    def code(self):
        return self._code

    def trailing_metadata(self):
        return ((ERROR_DETAIL_METADATA_KEY, b"\xff\xff\xff"),)


class _StringTrailerRpcError(grpc.RpcError):
    def __init__(self, code):
        super().__init__()
        self._code = code

    def code(self):
        return self._code

    def trailing_metadata(self):
        return ((ERROR_DETAIL_METADATA_KEY, "not-bytes"),)


class _OversizedTrailerRpcError(grpc.RpcError):
    def __init__(self, code):
        super().__init__()
        self._code = code

    def code(self):
        return self._code

    def trailing_metadata(self):
        return ((ERROR_DETAIL_METADATA_KEY, b"a" * (MAX_ERROR_DETAIL_TRAILER_BYTES + 1)),)


class _InitialOnlyRpcError(_FakeRpcError):
    def trailing_metadata(self):
        return ()

    def initial_metadata(self):
        return ((ERROR_DETAIL_METADATA_KEY, self._detail.SerializeToString()),)


def validate_live_proof_inputs(args: argparse.Namespace, validation_ready: bool, quota_ready: bool) -> None:
    if validation_ready:
        validate_method_path("validation proof", args.validation_method)
    if validation_ready and not str(args.validation_field or "").strip():
        raise ValueError("validation proof requires --validation-field so field_violations are asserted")
    if validation_ready:
        validate_expected_token("validation proof field", args.validation_field)
    if validation_ready and args.validation_status != VALIDATION_STATUS:
        raise ValueError(f"validation proof must expect INVALID_ARGUMENT, got {args.validation_status!r}")
    if quota_ready:
        validate_method_path("quota retry/backpressure proof", args.quota_method)
    if quota_ready and args.quota_status != QUOTA_STATUS:
        raise ValueError(f"quota retry/backpressure proof must expect RESOURCE_EXHAUSTED, got {args.quota_status!r}")
    if quota_ready and args.quota_retry_after_min_ms <= 0:
        raise ValueError("quota retry/backpressure proof requires --quota-retry-after-min-ms > 0")
    if quota_ready and not str(args.quota_backend or "").strip():
        raise ValueError("quota retry/backpressure proof requires --quota-backend so ErrorDetail.backend is asserted")
    if quota_ready:
        validate_expected_token("quota retry/backpressure proof backend", args.quota_backend)
    if quota_ready and not str(args.quota_operation or "").strip():
        raise ValueError("quota retry/backpressure proof requires --quota-operation so ErrorDetail.operation is asserted")
    if quota_ready:
        validate_expected_token("quota retry/backpressure proof operation", args.quota_operation)


def validate_expected_token(label: str, value: object) -> None:
    text = str(value or "")
    stripped = text.strip()
    if text != stripped:
        raise ValueError(f"{label} must not include surrounding whitespace")
    if any(char.isspace() for char in stripped):
        raise ValueError(f"{label} must not include whitespace")
    if _contains_control_character(stripped):
        raise ValueError(f"{label} must not include control characters")


def validate_required_expected_token(label: str, value: object) -> str:
    text = str(value or "")
    stripped = text.strip()
    if not stripped:
        raise ValueError(f"{label} must be non-empty")
    validate_expected_token(label, value)
    return text


def validate_live_check_expectations(
    label: str,
    expected_status: object,
    expected_kind: object,
    expected_retryable: object,
    min_retry_after_ms: object,
    expected_field: object,
    expected_backend: object,
    expected_operation: object,
) -> None:
    status = validate_required_expected_token(f"{label} expected status", expected_status)
    if status not in grpc.StatusCode.__members__:
        raise ValueError(f"{label} expected status must be a gRPC status code name")
    kind = validate_required_expected_token(f"{label} expected ErrorDetail.kind", expected_kind)
    try:
        ErrorKind.Value(kind)
    except ValueError as error:
        raise ValueError(f"{label} expected ErrorDetail.kind must be a known ErrorKind name") from error
    if expected_retryable is not None and not isinstance(expected_retryable, bool):
        raise ValueError(f"{label} expected retryable must be true, false, or unset")
    if min_retry_after_ms is not None:
        if not isinstance(min_retry_after_ms, int) or isinstance(min_retry_after_ms, bool) or min_retry_after_ms <= 0:
            raise ValueError(f"{label} expected retry_after_ms floor must be a positive integer")
    if expected_field is not None:
        validate_required_expected_token(f"{label} expected field", expected_field)
    if expected_backend is not None:
        validate_required_expected_token(f"{label} expected backend", expected_backend)
    if expected_operation is not None:
        validate_required_expected_token(f"{label} expected operation", expected_operation)
    if kind == "ERROR_KIND_VALIDATION":
        if status != VALIDATION_STATUS:
            raise ValueError(f"{label} validation runtime proof must expect {VALIDATION_STATUS}")
        if expected_retryable is not False:
            raise ValueError(f"{label} validation runtime proof must expect retryable=false")
        if min_retry_after_ms is not None:
            raise ValueError(f"{label} validation runtime proof must not expect retry_after_ms")
        if expected_field is None:
            raise ValueError(f"{label} validation runtime proof requires an expected field")
        if expected_backend is not None or expected_operation is not None:
            raise ValueError(f"{label} validation runtime proof must not expect backend/operation")
    elif kind == "ERROR_KIND_QUOTA":
        if status != QUOTA_STATUS:
            raise ValueError(f"{label} quota runtime proof must expect {QUOTA_STATUS}")
        if expected_retryable is not True:
            raise ValueError(f"{label} quota runtime proof must expect retryable=true")
        if min_retry_after_ms is None:
            raise ValueError(f"{label} quota runtime proof requires positive retry_after_ms floor")
        if expected_field is not None:
            raise ValueError(f"{label} quota runtime proof must not expect field_violations")
        if expected_backend is None or expected_operation is None:
            raise ValueError(f"{label} quota runtime proof requires expected backend and operation")
    else:
        raise ValueError(f"{label} runtime proof kind must be ERROR_KIND_VALIDATION or ERROR_KIND_QUOTA")


def validate_runtime_request_message(label: str, request: object) -> None:
    if not isinstance(request, Message):
        raise ValueError(f"{label} runtime request must be a protobuf message")


def validate_method_path(label: str, method: object) -> None:
    value = str(method or "")
    stripped = value.strip()
    if value != stripped:
        raise ValueError(f"{label} method must not include surrounding whitespace")
    if any(char.isspace() for char in stripped):
        raise ValueError(f"{label} method must not include whitespace")
    parts = stripped.split("/")
    if len(parts) != 3 or parts[0] != "" or not parts[1] or not parts[2] or "." not in parts[1]:
        raise ValueError(f"{label} method must be a full gRPC method path like /package.Service/Method")
    service_parts = parts[1].split(".")
    if any(not part.isidentifier() for part in service_parts) or not parts[2].isidentifier():
        raise ValueError(f"{label} method must use protobuf identifier tokens")


def _assert_validation_error(label: str, fn, needle: str) -> None:
    try:
        fn()
    except ValueError as error:
        if needle not in str(error):
            raise AssertionError(f"{label}: validation error {error!r} did not contain {needle!r}") from error
    else:
        raise AssertionError(f"{label}: live proof input validation selftest did not fail")


def run_selftest() -> None:
    validation = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(ErrorFieldViolation(field="tenant_id", description="required"),),
    )
    check_error_detail(
        "validation fixture",
        _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation),
        "INVALID_ARGUMENT",
        "ERROR_KIND_VALIDATION",
        expected_retryable=False,
        expected_field="tenant_id",
    )
    valid_upsert_request = UpsertRequest(message_type="Invoice")
    assert_request_matches_method("/udb.services.v1.DataBroker/Upsert", valid_upsert_request)

    try:
        assert_request_matches_method(
            "/udb.services.v1.DataBroker/Upsert",
            DeleteRequest(message_type="Invoice"),
        )
    except AssertionError as error:
        if "does not match RPC input" not in str(error):
            raise
    else:
        raise AssertionError("method/request descriptor mismatch regression was not caught")

    try:
        assert_request_matches_method(
            "/udb.services.v1.MissingService/Upsert",
            valid_upsert_request,
        )
    except AssertionError as error:
        if "generated service descriptor was not found" not in str(error):
            raise
    else:
        raise AssertionError("missing service descriptor regression was not caught")

    class _NoDialChannel:
        def unary_unary(self, *args, **kwargs):
            raise AssertionError("runtime method-path validation should run before dialing")

    try:
        run_live_check(
            "validation runtime malformed method",
            _NoDialChannel(),
            "udb.services.v1.DataBroker/Upsert",
            object(),
            (),
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "method must be a full gRPC method path like /package.Service/Method" not in str(error):
            raise
    else:
        raise AssertionError("runtime method-path validation regression was not caught")

    try:
        run_live_check(
            "quota runtime malformed backend",
            _NoDialChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            object(),
            (),
            1.0,
            QUOTA_STATUS,
            "ERROR_KIND_QUOTA",
            True,
            200,
            None,
            "ad mission",
            "fair_queue",
        )
    except AssertionError as error:
        if "expected backend must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("runtime expected-token validation regression was not caught")

    try:
        run_live_check(
            "validation runtime unknown kind",
            _NoDialChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            object(),
            (),
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_DOES_NOT_EXIST",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "expected ErrorDetail.kind must be a known ErrorKind name" not in str(error):
            raise
    else:
        raise AssertionError("runtime expected-kind validation regression was not caught")

    try:
        run_live_check(
            "validation runtime non-message request",
            _NoDialChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            object(),
            (),
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "runtime request must be a protobuf message" not in str(error):
            raise
    else:
        raise AssertionError("runtime request-message validation regression was not caught")

    try:
        run_live_check(
            "validation runtime raw metadata",
            _NoDialChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            [("authorization", "Bearer token")],
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "runtime metadata must be a parsed gRPC metadata tuple" not in str(error):
            raise
    else:
        raise AssertionError("runtime metadata validation regression was not caught")

    try:
        run_live_check(
            "validation runtime padded timeout",
            _NoDialChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            (),
            " 1.0 ",
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "runtime timeout is invalid" not in str(error):
            raise
    else:
        raise AssertionError("runtime timeout validation regression was not caught")

    try:
        run_live_check(
            "validation runtime missing channel method",
            object(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            (),
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "runtime channel must expose callable unary_unary" not in str(error):
            raise
    else:
        raise AssertionError("runtime channel-method validation regression was not caught")

    class _NonCallableUnaryChannel:
        def unary_unary(self, *args, **kwargs):
            return object()

    try:
        run_live_check(
            "validation runtime non-callable unary",
            _NonCallableUnaryChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            (),
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "runtime unary call must be callable" not in str(error):
            raise
    else:
        raise AssertionError("runtime unary-call validation regression was not caught")

    class _FailingUnaryFactoryChannel:
        def unary_unary(self, *args, **kwargs):
            raise RuntimeError("factory exploded")

    try:
        run_live_check(
            "validation runtime unary factory error",
            _FailingUnaryFactoryChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            (),
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "runtime unary factory raised error" not in str(error):
            raise
    else:
        raise AssertionError("runtime unary-factory validation regression was not caught")

    class _NonGrpcErrorUnaryChannel:
        def unary_unary(self, *args, **kwargs):
            def call(*call_args, **call_kwargs):
                raise RuntimeError("direct harness exploded")

            return call

    try:
        run_live_check(
            "validation runtime non-gRPC unary error",
            _NonGrpcErrorUnaryChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            (),
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "runtime unary call raised non-gRPC error" not in str(error):
            raise
    else:
        raise AssertionError("runtime unary non-gRPC error validation regression was not caught")

    try:
        run_live_check(
            "validation runtime weakened status",
            _NoDialChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            (),
            1.0,
            "FAILED_PRECONDITION",
            "ERROR_KIND_VALIDATION",
            False,
            None,
            "tenant_id",
        )
    except AssertionError as error:
        if "validation runtime proof must expect INVALID_ARGUMENT" not in str(error):
            raise
    else:
        raise AssertionError("runtime validation semantics regression was not caught")

    try:
        run_live_check(
            "validation runtime missing field",
            _NoDialChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            (),
            1.0,
            VALIDATION_STATUS,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            None,
        )
    except AssertionError as error:
        if "validation runtime proof requires an expected field" not in str(error):
            raise
    else:
        raise AssertionError("runtime validation field semantics regression was not caught")

    try:
        run_live_check(
            "quota runtime missing backend operation",
            _NoDialChannel(),
            "/udb.services.v1.DataBroker/Upsert",
            valid_upsert_request,
            (),
            1.0,
            QUOTA_STATUS,
            "ERROR_KIND_QUOTA",
            True,
            200,
            None,
        )
    except AssertionError as error:
        if "quota runtime proof requires expected backend and operation" not in str(error):
            raise
    else:
        raise AssertionError("runtime quota semantics regression was not caught")

    try:
        check_error_detail(
            "non-status-code fixture",
            _FakeRpcError("INVALID_ARGUMENT", validation),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "gRPC status code must be a grpc.StatusCode" not in str(error):
            raise
    else:
        raise AssertionError("non-grpc StatusCode regression was not caught")

    try:
        check_error_detail(
            "unreadable status-code fixture",
            _ThrowingCodeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "gRPC status code could not be read" not in str(error):
            raise
    else:
        raise AssertionError("unreadable status code regression was not caught")

    try:
        check_error_detail(
            "empty status message fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation, message=""),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "gRPC status message must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("empty status message regression was not caught")
    try:
        check_error_detail(
            "unreadable status message fixture",
            _ThrowingDetailsRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "gRPC status message could not be read" not in str(error):
            raise
    else:
        raise AssertionError("unreadable status message regression was not caught")
    try:
        check_error_detail(
            "non-string status message fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation, message=404),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "gRPC status message must be a string" not in str(error):
            raise
    else:
        raise AssertionError("non-string status message regression was not caught")
    try:
        check_error_detail(
            "padded status message fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation, message=" required "),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "gRPC status message must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("padded status message regression was not caught")
    try:
        check_error_detail(
            "control-character status message fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation, message="required\nx-injected: yes"),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "gRPC status message must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character status message regression was not caught")
    try:
        check_error_detail(
            "oversized status message fixture",
            _FakeRpcError(
                grpc.StatusCode.INVALID_ARGUMENT,
                validation,
                message="a" * (MAX_STATUS_MESSAGE_BYTES + 1),
            ),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "gRPC status message must be <=" not in str(error):
            raise
    else:
        raise AssertionError("oversized status message regression was not caught")
    try:
        decode_error_detail(_DuplicateTrailerRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "expected exactly one udb-error-detail-bin trailer" not in str(error):
            raise
    else:
        raise AssertionError("duplicate ErrorDetail trailer regression was not caught")
    try:
        decode_error_detail(_NonStringTrailerKeyRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "udb-error-detail-bin trailer metadata key must be a string" not in str(error):
            raise
    else:
        raise AssertionError("non-string ErrorDetail trailer metadata key regression was not caught")
    try:
        decode_error_detail(_UppercaseTrailerKeyRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "udb-error-detail-bin trailer metadata key must be lowercase" not in str(error):
            raise
    else:
        raise AssertionError("uppercase ErrorDetail trailer metadata key regression was not caught")
    try:
        decode_error_detail(_ThrowingTrailerMetadataRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "udb-error-detail-bin trailer metadata could not be read" not in str(error):
            raise
    else:
        raise AssertionError("unreadable ErrorDetail trailer metadata regression was not caught")
    try:
        decode_error_detail(_FailingTrailerMetadataIteratorRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "udb-error-detail-bin trailer metadata iteration failed" not in str(error):
            raise
    else:
        raise AssertionError("failing ErrorDetail trailer metadata iterator regression was not caught")
    try:
        decode_error_detail(_NonIterableTrailerMetadataRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "udb-error-detail-bin trailer metadata must be iterable" not in str(error):
            raise
    else:
        raise AssertionError("non-iterable ErrorDetail trailer metadata regression was not caught")
    try:
        decode_error_detail(_FailingTrailerMetadataItemRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "udb-error-detail-bin trailer metadata item could not be read" not in str(error):
            raise
    else:
        raise AssertionError("failing ErrorDetail trailer metadata item regression was not caught")
    try:
        decode_error_detail(_MalformedTrailerMetadataItemRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "udb-error-detail-bin trailer metadata item must be a key/value pair" not in str(error):
            raise
    else:
        raise AssertionError("malformed ErrorDetail trailer metadata item regression was not caught")
    try:
        decode_error_detail(_MalformedTrailerRpcError(grpc.StatusCode.INVALID_ARGUMENT))
    except AssertionError as error:
        if "invalid udb-error-detail-bin trailer" not in str(error):
            raise
    else:
        raise AssertionError("malformed ErrorDetail trailer regression was not caught")
    try:
        decode_error_detail(_StringTrailerRpcError(grpc.StatusCode.INVALID_ARGUMENT))
    except AssertionError as error:
        if "udb-error-detail-bin trailer must be bytes" not in str(error):
            raise
    else:
        raise AssertionError("string ErrorDetail trailer regression was not caught")
    try:
        decode_error_detail(_OversizedTrailerRpcError(grpc.StatusCode.INVALID_ARGUMENT))
    except AssertionError as error:
        if "udb-error-detail-bin trailer must be <=" not in str(error):
            raise
    else:
        raise AssertionError("oversized ErrorDetail trailer regression was not caught")
    try:
        decode_error_detail(_InitialOnlyRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation))
    except AssertionError as error:
        if "missing udb-error-detail-bin trailer" not in str(error):
            raise
    else:
        raise AssertionError("initial-metadata ErrorDetail regression was not caught")
    unknown_kind = ErrorDetail(
        kind=99,
        retryable=False,
        field_violations=(ErrorFieldViolation(field="tenant_id", description="required"),),
    )
    try:
        check_error_detail(
            "unknown kind fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, unknown_kind),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "got unknown ErrorDetail.kind 99" not in str(error):
            raise
    else:
        raise AssertionError("unknown ErrorDetail kind regression was not caught")

    quota = ErrorDetail(
        backend="admission",
        operation="fair_queue",
        kind=ErrorKind.ERROR_KIND_QUOTA,
        retryable=True,
        retry_after_ms=250,
    )
    check_error_detail(
        "quota fixture",
        _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, quota),
        "RESOURCE_EXHAUSTED",
        "ERROR_KIND_QUOTA",
        expected_retryable=True,
        min_retry_after_ms=200,
        expected_backend="admission",
        expected_operation="fair_queue",
    )
    try:
        check_error_detail(
            "quota identity fixture",
            _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, quota),
            "RESOURCE_EXHAUSTED",
            "ERROR_KIND_QUOTA",
            expected_retryable=True,
            min_retry_after_ms=200,
            expected_backend="storage",
            expected_operation="tenant_bytes",
        )
    except AssertionError as error:
        if "got ErrorDetail.backend 'admission', want 'storage'" not in str(error):
            raise
    else:
        raise AssertionError("quota backend/operation regression was not caught")
    empty_backend_quota = ErrorDetail(
        backend="",
        operation="fair_queue",
        kind=ErrorKind.ERROR_KIND_QUOTA,
        retryable=True,
        retry_after_ms=250,
    )
    try:
        check_error_detail(
            "empty quota backend fixture",
            _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, empty_backend_quota),
            "RESOURCE_EXHAUSTED",
            "ERROR_KIND_QUOTA",
            expected_retryable=True,
            min_retry_after_ms=200,
            expected_backend="admission",
            expected_operation="fair_queue",
        )
    except AssertionError as error:
        if "ErrorDetail.backend must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("quota backend token regression was not caught")
    padded_operation_quota = ErrorDetail(
        backend="admission",
        operation=" fair_queue ",
        kind=ErrorKind.ERROR_KIND_QUOTA,
        retryable=True,
        retry_after_ms=250,
    )
    try:
        check_error_detail(
            "padded quota operation fixture",
            _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, padded_operation_quota),
            "RESOURCE_EXHAUSTED",
            "ERROR_KIND_QUOTA",
            expected_retryable=True,
            min_retry_after_ms=200,
            expected_backend="admission",
            expected_operation="fair_queue",
        )
    except AssertionError as error:
        if "ErrorDetail.operation must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("quota operation token regression was not caught")
    control_backend_quota = ErrorDetail(
        backend="admission\0",
        operation="fair_queue",
        kind=ErrorKind.ERROR_KIND_QUOTA,
        retryable=True,
        retry_after_ms=250,
    )
    try:
        check_error_detail(
            "control-character quota backend fixture",
            _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, control_backend_quota),
            "RESOURCE_EXHAUSTED",
            "ERROR_KIND_QUOTA",
            expected_retryable=True,
            min_retry_after_ms=200,
            expected_backend="admission",
            expected_operation="fair_queue",
        )
    except AssertionError as error:
        if "ErrorDetail.backend must not include control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character quota backend token regression was not caught")
    non_retryable_quota = ErrorDetail(kind=ErrorKind.ERROR_KIND_QUOTA, retryable=False, retry_after_ms=250)
    try:
        check_error_detail(
            "non-retryable quota fixture",
            _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, non_retryable_quota),
            "RESOURCE_EXHAUSTED",
            "ERROR_KIND_QUOTA",
            expected_retryable=True,
            min_retry_after_ms=200,
        )
    except AssertionError as error:
        if "got retryable=False, want True" not in str(error):
            raise
    else:
        raise AssertionError("quota retryable regression was not caught")
    low_retry_after_quota = ErrorDetail(kind=ErrorKind.ERROR_KIND_QUOTA, retryable=True, retry_after_ms=100)
    try:
        check_error_detail(
            "low retry-after quota fixture",
            _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, low_retry_after_quota),
            "RESOURCE_EXHAUSTED",
            "ERROR_KIND_QUOTA",
            expected_retryable=True,
            min_retry_after_ms=200,
        )
    except AssertionError as error:
        if "got retry_after_ms=100, want >= 200" not in str(error):
            raise
    else:
        raise AssertionError("quota retry-after floor regression was not caught")
    quota_with_fields = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_QUOTA,
        retryable=True,
        retry_after_ms=250,
        field_violations=(ErrorFieldViolation(field="tenant_id", description="required"),),
    )
    try:
        check_error_detail(
            "quota with field violations fixture",
            _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, quota_with_fields),
            "RESOURCE_EXHAUSTED",
            "ERROR_KIND_QUOTA",
            expected_retryable=True,
            min_retry_after_ms=200,
        )
    except AssertionError as error:
        if "quota/backpressure detail must not include field_violations" not in str(error):
            raise
    else:
        raise AssertionError("quota field-violations regression was not caught")

    try:
        check_error_detail(
            "missing field fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="project_id",
        )
    except AssertionError as error:
        if "do not include 'project_id'" not in str(error):
            raise
    else:
        raise AssertionError("missing field regression was not caught")
    extra_valid_field = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(
            ErrorFieldViolation(field="tenant_id", description="required"),
            ErrorFieldViolation(field="project_id", description="required"),
        ),
    )
    try:
        check_error_detail(
            "extra valid field fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, extra_valid_field),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "validation proof must include exactly 'tenant_id'" not in str(error):
            raise
    else:
        raise AssertionError("extra validation field regression was not caught")
    empty_description = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(ErrorFieldViolation(field="tenant_id", description=""),),
    )
    try:
        check_error_detail(
            "empty field description fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, empty_description),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "must include a non-empty description" not in str(error):
            raise
    else:
        raise AssertionError("empty field description regression was not caught")
    malformed_extra_field = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(
            ErrorFieldViolation(field="tenant_id", description="required"),
            ErrorFieldViolation(field="", description="malformed"),
        ),
    )
    try:
        check_error_detail(
            "malformed extra field fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, malformed_extra_field),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "field_violations[1].field must be non-empty" not in str(error):
            raise
    else:
        raise AssertionError("malformed extra field violation regression was not caught")
    spaced_field = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(ErrorFieldViolation(field=" tenant_id ", description="required"),),
    )
    try:
        check_error_detail(
            "spaced field fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, spaced_field),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "field_violations[0].field must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("spaced field violation regression was not caught")
    embedded_space_field = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(ErrorFieldViolation(field="tenant id", description="required"),),
    )
    try:
        check_error_detail(
            "embedded-space field fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, embedded_space_field),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "field_violations[0].field must not include whitespace" not in str(error):
            raise
    else:
        raise AssertionError("embedded-space field violation regression was not caught")
    control_field = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(ErrorFieldViolation(field="tenant_id\0", description="required"),),
    )
    try:
        check_error_detail(
            "control-character field fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, control_field),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "field_violations[0].field must not include control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character field violation regression was not caught")
    padded_description = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(ErrorFieldViolation(field="tenant_id", description=" required "),),
    )
    try:
        check_error_detail(
            "padded description fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, padded_description),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "description must not include surrounding whitespace" not in str(error):
            raise
    else:
        raise AssertionError("padded field description regression was not caught")
    control_description = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(ErrorFieldViolation(field="tenant_id", description="required\nx-injected: yes"),),
    )
    try:
        check_error_detail(
            "control-character description fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, control_description),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "description must not contain control characters" not in str(error):
            raise
    else:
        raise AssertionError("control-character field description regression was not caught")
    oversized_description = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(
            ErrorFieldViolation(
                field="tenant_id",
                description="a" * (MAX_FIELD_VIOLATION_DESCRIPTION_BYTES + 1),
            ),
        ),
    )
    try:
        check_error_detail(
            "oversized description fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, oversized_description),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "description must be <= 8192 bytes" not in str(error):
            raise
    else:
        raise AssertionError("oversized field description regression was not caught")
    validation_with_retry_after = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        retry_after_ms=250,
        field_violations=(ErrorFieldViolation(field="tenant_id", description="required"),),
    )
    try:
        check_error_detail(
            "validation retry-after fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation_with_retry_after),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "validation detail must not include retry_after_ms=250" not in str(error):
            raise
    else:
        raise AssertionError("validation retry-after regression was not caught")
    retryable_validation = ErrorDetail(
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=True,
        field_violations=(ErrorFieldViolation(field="tenant_id", description="required"),),
    )
    try:
        check_error_detail(
            "validation retryable fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, retryable_validation),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "got retryable=True, want False" not in str(error):
            raise
    else:
        raise AssertionError("validation retryable regression was not caught")
    validation_with_backend = ErrorDetail(
        backend="admission",
        operation="fair_queue",
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        retryable=False,
        field_violations=(ErrorFieldViolation(field="tenant_id", description="required"),),
    )
    try:
        check_error_detail(
            "validation backend fixture",
            _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, validation_with_backend),
            "INVALID_ARGUMENT",
            "ERROR_KIND_VALIDATION",
            expected_retryable=False,
            expected_field="tenant_id",
        )
    except AssertionError as error:
        if "validation detail must not include backend/operation" not in str(error):
            raise
    else:
        raise AssertionError("validation backend/operation regression was not caught")
    _assert_validation_error(
        "validation proof missing field",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_method="/udb.services.v1.DataBroker/Upsert",
                validation_field="",
                validation_status=VALIDATION_STATUS,
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
            ),
            validation_ready=True,
            quota_ready=False,
        ),
        "requires --validation-field",
    )
    _assert_validation_error(
        "validation proof malformed method path",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_method="udb.services.v1.DataBroker/Upsert",
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=True,
            quota_ready=False,
        ),
        "must be a full gRPC method path",
    )
    _assert_validation_error(
        "validation proof method path has embedded whitespace",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_method="/udb.services.v1.Data Broker/Upsert",
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=True,
            quota_ready=False,
        ),
        "method must not include whitespace",
    )
    _assert_validation_error(
        "validation proof method path has malformed token",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_method="/udb.services.v1.DataBroker/Bad-Method",
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=True,
            quota_ready=False,
        ),
        "method must use protobuf identifier tokens",
    )
    _assert_validation_error(
        "validation proof field has surrounding whitespace",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_method="/udb.services.v1.DataBroker/Upsert",
                validation_field=" tenant_id ",
                validation_status=VALIDATION_STATUS,
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=True,
            quota_ready=False,
        ),
        "field must not include surrounding whitespace",
    )
    _assert_validation_error(
        "validation proof field has control character",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_method="/udb.services.v1.DataBroker/Upsert",
                validation_field="tenant_id\0",
                validation_status=VALIDATION_STATUS,
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=True,
            quota_ready=False,
        ),
        "field must not include control characters",
    )
    _assert_validation_error(
        "quota proof missing positive retry-after",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=0,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "requires --quota-retry-after-min-ms > 0",
    )
    _assert_validation_error(
        "quota proof method path has surrounding whitespace",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method=" /udb.services.v1.DataBroker/Upsert ",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "method must not include surrounding whitespace",
    )
    _assert_validation_error(
        "quota proof method path has embedded whitespace",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Up sert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "method must not include whitespace",
    )
    _assert_validation_error(
        "quota proof method path has malformed token",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Bad-Method",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "method must use protobuf identifier tokens",
    )
    _assert_validation_error(
        "validation proof status weakened",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_method="/udb.services.v1.DataBroker/Upsert",
                validation_field="tenant_id",
                validation_status="FAILED_PRECONDITION",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=True,
            quota_ready=False,
        ),
        f"must expect {VALIDATION_STATUS}",
    )
    _assert_validation_error(
        "quota proof status weakened",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status="UNAVAILABLE",
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        f"must expect {QUOTA_STATUS}",
    )
    _assert_validation_error(
        "quota proof missing backend",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="",
                quota_operation="fair_queue",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "requires --quota-backend",
    )
    _assert_validation_error(
        "quota proof missing operation",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "requires --quota-operation",
    )
    _assert_validation_error(
        "quota proof backend has embedded whitespace",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="ad mission",
                quota_operation="fair_queue",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "backend must not include whitespace",
    )
    _assert_validation_error(
        "quota proof backend has control character",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission\0",
                quota_operation="fair_queue",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "backend must not include control characters",
    )
    _assert_validation_error(
        "quota proof operation has surrounding whitespace",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation=" fair_queue ",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "operation must not include surrounding whitespace",
    )
    _assert_validation_error(
        "quota proof operation has control character",
        lambda: validate_live_proof_inputs(
            argparse.Namespace(
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue\0",
            ),
            validation_ready=False,
            quota_ready=True,
        ),
        "operation must not include control characters",
    )
    missing = missing_required_live_proofs(
        argparse.Namespace(
            validation_method="   ",
            validation_request_module="udb.services.v1.data_broker_pb2",
            validation_request_message="UpsertRequest",
            validation_request_json="validation.json",
            validation_field="tenant_id",
            quota_method="/udb.services.v1.DataBroker/Upsert",
            quota_request_module="udb.services.v1.data_broker_pb2",
            quota_request_message="UpsertRequest",
            quota_request_json="quota.json",
            quota_retry_after_min_ms=0,
            quota_backend="admission",
            quota_operation="fair_queue",
        )
    )
    if missing != ["validation method", "quota retry-after floor"]:
        raise AssertionError(f"whitespace-only required proof input regression was not caught: {missing!r}")
    if _complete("   ", "udb.services.v1.data_broker_pb2"):
        raise AssertionError("whitespace-only focused proof readiness regression was not caught")
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        valid_request = temp / "valid.json"
        valid_request.write_text("{}", encoding="utf-8")
        load_request("google.protobuf.empty_pb2", "Empty", valid_request)
        _assert_validation_error(
            "missing request JSON file",
            lambda: load_request("google.protobuf.empty_pb2", "Empty", temp / "missing.json", "validation request"),
            "proof file must exist",
        )
        _assert_validation_error(
            "missing request module",
            lambda: load_request("udb.missing_pb2", "Empty", valid_request, "validation request"),
            "could not be imported",
        )
        _assert_validation_error(
            "spaced request module",
            lambda: load_request(" google.protobuf.empty_pb2", "Empty", valid_request, "validation request"),
            "request module must not include surrounding whitespace",
        )
        _assert_validation_error(
            "malformed request module",
            lambda: load_request("google.protobuf..empty_pb2", "Empty", valid_request, "validation request"),
            "request module must be a dotted Python module path",
        )
        _assert_validation_error(
            "missing request message",
            lambda: load_request("google.protobuf.empty_pb2", "MissingRequest", valid_request, "validation request"),
            "does not expose",
        )
        _assert_validation_error(
            "spaced request message",
            lambda: load_request("google.protobuf.empty_pb2", " Empty", valid_request, "validation request"),
            "request message must not include surrounding whitespace",
        )
        _assert_validation_error(
            "malformed request message",
            lambda: load_request("google.protobuf.empty_pb2", "Bad-Message", valid_request, "validation request"),
            "request message must be a Python identifier",
        )
        _assert_validation_error(
            "non-message request symbol",
            lambda: load_request("google.protobuf.empty_pb2", "DESCRIPTOR", valid_request, "validation request"),
            "does not expose protobuf message class",
        )
        oversized_request = temp / "oversized.json"
        oversized_request.write_text(" " * (MAX_PROOF_INPUT_BYTES + 1), encoding="utf-8")
        _assert_validation_error(
            "oversized request JSON file",
            lambda: load_request("google.protobuf.empty_pb2", "Empty", oversized_request, "validation request"),
            "proof file must be <=",
        )

        array_request = temp / "array.json"
        array_request.write_text("[]", encoding="utf-8")
        _assert_validation_error(
            "array request JSON",
            lambda: load_request("google.protobuf.empty_pb2", "Empty", array_request),
            "request JSON must be a JSON object",
        )

        malformed_request = temp / "malformed.json"
        malformed_request.write_text("{", encoding="utf-8")
        _assert_validation_error(
            "malformed request JSON",
            lambda: load_request("google.protobuf.empty_pb2", "Empty", malformed_request),
            "request JSON must be a valid JSON object",
        )

        duplicate_key_request = temp / "duplicate-key.json"
        duplicate_key_request.write_text('{"name":"a","name":"b"}', encoding="utf-8")
        _assert_validation_error(
            "duplicate-key request JSON",
            lambda: load_request("google.protobuf.empty_pb2", "Empty", duplicate_key_request),
            "request JSON must not contain duplicate key",
        )

        non_finite_request = temp / "non-finite.json"
        non_finite_request.write_text('{"name":NaN}', encoding="utf-8")
        _assert_validation_error(
            "non-finite request JSON",
            lambda: load_request("google.protobuf.empty_pb2", "Empty", non_finite_request),
            "request JSON must not contain non-standard constant NaN",
        )
    try:
        validate_live_proof_inputs(
            argparse.Namespace(
                validation_method="/udb.services.v1.DataBroker/Upsert",
                validation_field="tenant_id",
                validation_status=VALIDATION_STATUS,
                quota_method="/udb.services.v1.DataBroker/Upsert",
                quota_status=QUOTA_STATUS,
                quota_retry_after_min_ms=200,
                quota_backend="admission",
                quota_operation="fair_queue",
            ),
            validation_ready=True,
            quota_ready=True,
        )
    except ValueError as error:
        raise AssertionError("valid live proof input validation failed") from error
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
    print("error detail served smoke selftest passed")


def _present(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return value is not None and bool(str(value).strip())


def _complete(*values: object) -> bool:
    return all(_present(value) for value in values)


def missing_required_live_proofs(args: argparse.Namespace) -> list[str]:
    return [label for attr, label in REQUIRED_LIVE_PROOF_INPUTS if not _present(getattr(args, attr))]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run local fake-RpcError assertions")
    parser.add_argument("--target", help="live broker gRPC target, for example 127.0.0.1:50051")
    parser.add_argument("--tls", action="store_true", help="use TLS for the gRPC channel")
    parser.add_argument("--header", action="append", default=[], help="extra gRPC metadata header as 'Name: Value'")
    parser.add_argument("--timeout", default="10.0", help="per-RPC timeout in seconds")

    parser.add_argument("--validation-method", help="full unary method path for validation proof")
    parser.add_argument("--validation-request-module", help="generated Python pb2 module for validation request")
    parser.add_argument("--validation-request-message", help="generated Python request message class")
    parser.add_argument("--validation-request-json", type=Path, help="validation request JSON expected to fail")
    parser.add_argument("--validation-status", default=VALIDATION_STATUS, help="expected validation gRPC status")
    parser.add_argument("--validation-field", help="expected ErrorDetail.field_violations field path")

    parser.add_argument("--quota-method", help="full unary method path for quota/backpressure proof")
    parser.add_argument("--quota-request-module", help="generated Python pb2 module for quota request")
    parser.add_argument("--quota-request-message", help="generated Python request message class")
    parser.add_argument("--quota-request-json", type=Path, help="quota request JSON expected to fail")
    parser.add_argument("--quota-status", default=QUOTA_STATUS, help="expected quota gRPC status")
    parser.add_argument("--quota-retry-after-min-ms", type=int, default=0, help="minimum retry_after_ms")
    parser.add_argument("--quota-backend", help="expected ErrorDetail.backend for quota/backpressure proof")
    parser.add_argument("--quota-operation", help="expected ErrorDetail.operation for quota/backpressure proof")
    parser.add_argument("--require-all-proofs", action="store_true", help="require every Chapter 14.7 live proof input")
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        return 0
    if not args.target:
        parser.error("--target is required outside --selftest")

    validation_ready = _complete(
        args.validation_method,
        args.validation_request_module,
        args.validation_request_message,
        args.validation_request_json,
    )
    quota_ready = _complete(args.quota_method, args.quota_request_module, args.quota_request_message, args.quota_request_json)
    if not validation_ready and not quota_ready:
        parser.error("provide either the validation proof inputs or the quota proof inputs")
    if args.require_all_proofs:
        missing = missing_required_live_proofs(args)
        if missing:
            parser.error("missing required ErrorDetail live proof inputs: " + ", ".join(missing))
    try:
        validate_live_proof_inputs(args, validation_ready, quota_ready)
    except ValueError as error:
        parser.error(str(error))

    try:
        metadata = _parse_headers(args.header)
        target = validate_grpc_target(args.target)
        timeout = validate_timeout_seconds(args.timeout)
    except ValueError as error:
        parser.error(str(error))
    try:
        validation_request = (
            load_request(
                args.validation_request_module,
                args.validation_request_message,
                args.validation_request_json,
                "validation request",
            )
            if validation_ready
            else None
        )
        quota_request = (
            load_request(
                args.quota_request_module,
                args.quota_request_message,
                args.quota_request_json,
                "quota request",
            )
            if quota_ready
            else None
        )
    except ValueError as error:
        parser.error(str(error))
    channel = make_channel(target, args.tls)
    checked: list[str] = []
    if validation_request is not None:
        run_live_check(
            "validation served error detail",
            channel,
            args.validation_method,
            validation_request,
            metadata,
            timeout,
            args.validation_status,
            "ERROR_KIND_VALIDATION",
            False,
            None,
            args.validation_field,
        )
        checked.append("validation field violations")
    if quota_request is not None:
        run_live_check(
            "quota served error detail",
            channel,
            args.quota_method,
            quota_request,
            metadata,
            timeout,
            args.quota_status,
            "ERROR_KIND_QUOTA",
            True,
            args.quota_retry_after_min_ms,
            None,
            args.quota_backend,
            args.quota_operation,
        )
        checked.append("quota retry/backpressure")
    print(f"error detail served smoke passed: {', '.join(checked)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
