"""An in-memory udb broker for unit tests.

It serves the DataBroker data verbs (Select, Insert, Upsert, Update, Delete), atomic
transactions, durable CDC consumers and operator DLQ APIs, plus the API-key exchange,
over a real loopback gRPC listener, so the code under test uses an unmodified
``UdbProject``. Its semantics follow the broker's: the caller's tenant is filled into
filters and records (a different tenant is refused), primary keys are unique,
conditional writes compare by value, ``require_affected`` is enforced, reads page with
``has_more`` and exact totals, and every refusal carries the broker's stable ``UDB_*``
reason in the typed error detail. A service's tests catch what production would.

The same conformance suite runs against this fake and a live broker, which keeps the
two from drifting. See :mod:`udb_client.udbtest.conformance`.
"""

from __future__ import annotations

import base64
from concurrent import futures
import copy
from dataclasses import dataclass, field, replace
from fractions import Fraction
from functools import cmp_to_key
import hashlib
import json
import math
import re
import secrets
import threading
import time
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence
import unicodedata
import uuid

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.struct_pb2 import ListValue, Struct, Value

from udb.core.authn.services.v1 import core_pb2 as authn_pb2
from udb.entity.v1 import error_pb2, types_pb2
from udb.events.v1 import udb_events_pb2

from ..client import to_struct
from ..record_codec import _is_json, _loads
from ..project import UdbAsyncProject, UdbConfig, UdbProject
from ..record_codec import _INT64_TYPES, decode_field, encode_field
from ..schema import (
    column_options,
    is_encrypted,
    is_masked,
    is_pii,
    is_repeated,
    message_type,
    primary_keys,
    project_column,
    table_options,
    tenant_column,
)
from ..tx import EVENT_ENVELOPE_VERSION

_MASK = "***MASKED***"
_REDACTED = "***REDACTED***"
_DETAIL_TRAILER = "udb-error-detail-bin"
_STATUS_OK = grpc.StatusCode.OK
_INTEGER_SQL = frozenset({"SMALLINT", "INTEGER", "INT", "INT2", "INT4", "INT8", "BIGINT", "SERIAL", "BIGSERIAL"})
_OPERATORS = frozenset(
    {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$between",
     "$is_null", "$not_null", "$not", "$like", "$ilike"}
)
_DLQ_METHODS = frozenset({"ListDlqEvents", "GetDlqEvent", "ReplayDlqEvent", "DismissDlqEvent", "QuarantineDlqEvent"})
_RFC3339 = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?([Zz]|[+-]\d{2}:\d{2})$")
_CONSUMER_CHARS = re.compile(r"^[A-Za-z0-9_.:-]+$")
_UUID_TEXT = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

_FALSE, _TRUE, _UNKNOWN = 0, 1, 2


class _Refusal(Exception):
    """A refusal with the broker's typed ErrorDetail."""

    def __init__(
        self,
        code: grpc.StatusCode,
        reason: str,
        message: str,
        detail: error_pb2.ErrorDetail | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.reason = reason
        self.message = message
        self.detail = detail


def _refuse(code: grpc.StatusCode, reason: str, message: str) -> _Refusal:
    return _Refusal(code, reason, message)


def _invalid(message: str, reason: str = "") -> _Refusal:
    return _Refusal(grpc.StatusCode.INVALID_ARGUMENT, reason, message)


def _error_kind(code: grpc.StatusCode, reason: str) -> int:
    by_reason = {
        "UDB_CAS_CONFLICT": error_pb2.ERROR_KIND_CONFLICT,
        "UDB_CAS_ROW_MISSING": error_pb2.ERROR_KIND_CONFLICT,
        "UDB_CAS_KEY_NOT_PK": error_pb2.ERROR_KIND_VALIDATION,
        "UDB_TABLE_NOT_TENANT_SCOPED": error_pb2.ERROR_KIND_VALIDATION,
        "UDB_NULL_COMPARISON": error_pb2.ERROR_KIND_VALIDATION,
        "UDB_UNSUPPORTED_FILTER_OPERATOR": error_pb2.ERROR_KIND_VALIDATION,
        "UDB_UNIQUE_VIOLATION": error_pb2.ERROR_KIND_UNIQUE,
        "UDB_NOT_NULL_VIOLATION": error_pb2.ERROR_KIND_NOT_NULL,
        "UDB_SCOPE_MISSING": error_pb2.ERROR_KIND_PERMISSION,
        "UDB_TENANT_MISMATCH": error_pb2.ERROR_KIND_PERMISSION,
        "UDB_PROJECT_MISMATCH": error_pb2.ERROR_KIND_PERMISSION,
        "UDB_POLICY_DENIED": error_pb2.ERROR_KIND_POLICY,
        "UDB_UNKNOWN_MESSAGE_TYPE": error_pb2.ERROR_KIND_SCHEMA,
        "UDB_RATE_LIMITED": error_pb2.ERROR_KIND_RATE_LIMITED,
        "UDB_NO_ROWS_AFFECTED": error_pb2.ERROR_KIND_NOT_FOUND,
    }
    if reason in by_reason:
        return by_reason[reason]
    return {
        grpc.StatusCode.UNAVAILABLE: error_pb2.ERROR_KIND_RETRYABLE,
        grpc.StatusCode.INTERNAL: error_pb2.ERROR_KIND_INTERNAL,
        grpc.StatusCode.NOT_FOUND: error_pb2.ERROR_KIND_NOT_FOUND,
        grpc.StatusCode.INVALID_ARGUMENT: error_pb2.ERROR_KIND_VALIDATION,
        grpc.StatusCode.PERMISSION_DENIED: error_pb2.ERROR_KIND_POLICY,
    }.get(code, error_pb2.ERROR_KIND_UNSPECIFIED)


def _column_refusal(column: str) -> _Refusal:
    reason = "UDB_NOT_NULL_VIOLATION"
    return _Refusal(
        grpc.StatusCode.INVALID_ARGUMENT, reason, f"required column is null: {column}",
        error_pb2.ErrorDetail(reason=reason, kind=error_pb2.ERROR_KIND_NOT_NULL, column=column),
    )


def _unique_refusal(name: str) -> _Refusal:
    reason = "UDB_UNIQUE_VIOLATION"
    return _Refusal(
        grpc.StatusCode.ALREADY_EXISTS, reason, "write conflicts with a declared unique key",
        error_pb2.ErrorDetail(reason=reason, kind=error_pb2.ERROR_KIND_UNIQUE, constraint=name),
    )


def _missing_scope(scope: str) -> _Refusal:
    reason = "UDB_SCOPE_MISSING"
    return _Refusal(
        grpc.StatusCode.PERMISSION_DENIED, reason, f"verified identity lacks {scope}",
        error_pb2.ErrorDetail(reason=reason, kind=error_pb2.ERROR_KIND_PERMISSION, missing={"scope": scope}),
    )


def _violation(code: grpc.StatusCode, reason: str, message: str, kind: int, field_name: str, description: str, fix_hint: str = "") -> _Refusal:
    return _Refusal(
        code, reason, message,
        error_pb2.ErrorDetail(
            reason=reason, kind=kind, fix_hint=fix_hint,
            field_violations=[error_pb2.ErrorFieldViolation(field=field_name, description=description)],
        ),
    )


# ── value semantics ──────────────────────────────────────────────────────────


def _value_of(value: Value) -> Any:
    kind = value.WhichOneof("kind")
    if kind == "struct_value":
        return _struct_map(value.struct_value)
    if kind == "list_value":
        return _list_of(value.list_value)
    if kind == "number_value":
        return value.number_value
    if kind == "string_value":
        return value.string_value
    if kind == "bool_value":
        return value.bool_value
    return None


def _list_of(values: ListValue) -> list[Any]:
    return [_value_of(item) for item in values.values]


def _struct_map(struct: Struct) -> dict[str, Any]:
    """A Struct as a dict; every number stays a float, as it is on the wire."""
    return {key: _value_of(value) for key, value in struct.fields.items()}


def _numeric(value: Any) -> Fraction | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, float) and math.isfinite(value):
        return Fraction(repr(value))
    return None


def _text_number(text: Any) -> Fraction | None:
    if not isinstance(text, str):
        return None
    try:
        return Fraction(text)
    except (ValueError, ZeroDivisionError):
        return None


def _number_pair(a: Any, b: Any) -> tuple[Fraction, Fraction] | None:
    left, right = _numeric(a), _numeric(b)
    if left is not None and right is None:
        right = _text_number(b)
    if right is not None and left is None:
        left = _text_number(a)
    if left is None or right is None:
        return None
    return left, right


def _time_nanos(text: Any) -> int | None:
    match = _RFC3339.match(text) if isinstance(text, str) else None
    if match is None:
        return None
    year, month, day, hour, minute, second, fraction, zone = match.groups()
    try:
        from datetime import datetime, timezone

        base = int(datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=timezone.utc).timestamp())
    except ValueError:
        return None
    if zone not in ("Z", "z"):
        sign = -1 if zone[0] == "-" else 1
        base -= sign * (int(zone[1:3]) * 3600 + int(zone[4:6]) * 60)
    return base * 1_000_000_000 + int((fraction or "").ljust(9, "0") or "0")


def _json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _equal(a: Any, b: Any) -> bool:
    pair = _number_pair(a, b)
    if pair is not None:
        return pair[0] == pair[1]
    if isinstance(a, str) and isinstance(b, str):
        left, right = _time_nanos(a), _time_nanos(b)
        if left is not None and right is not None:
            return left == right
        return a == b
    return _json_text(a) == _json_text(b)


def _sprint(value: Any) -> str:
    if value is None:
        return "<nil>"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _compare(a: Any, b: Any) -> int:
    pair = _number_pair(a, b)
    if pair is not None:
        return (pair[0] > pair[1]) - (pair[0] < pair[1])
    left, right = _time_nanos(a), _time_nanos(b)
    if left is not None and right is not None:
        return (left > right) - (left < right)
    text_a, text_b = _sprint(a), _sprint(b)
    return (text_a > text_b) - (text_a < text_b)


def _like(text: str, pattern: str) -> bool:
    if not pattern:
        return not text
    if pattern[0] == "%":
        return any(_like(text[i:], pattern[1:]) for i in range(len(text) + 1))
    if pattern[0] == "_":
        return bool(text) and _like(text[1:], pattern[1:])
    return bool(text) and text[0] == pattern[0] and _like(text[1:], pattern[1:])


def _validate_operators(condition: Any) -> None:
    if not isinstance(condition, dict):
        return
    for op, arg in condition.items():
        name = op.lower()
        if name not in _OPERATORS:
            raise _invalid(f"unsupported filter operator {op}", "UDB_UNSUPPORTED_FILTER_OPERATOR")
        if name in ("$eq", "$ne") and arg is None:
            raise _invalid("use $is_null or $not_null instead of comparing NULL", "UDB_NULL_COMPARISON")
        if name in ("$in", "$nin", "$between"):
            if not isinstance(arg, list) or (name == "$between" and len(arg) != 2):
                raise _invalid("filter operator requires an array of values")
        if name == "$not":
            _validate_operators(arg)


def _validate_filter(filter_: Mapping[str, Any]) -> None:
    for key, condition in filter_.items():
        if key.strip().lower() in ("$and", "$or"):
            if not isinstance(condition, list) or not condition:
                raise _invalid("boolean filter requires a nonempty array of objects")
            for item in condition:
                if not isinstance(item, dict) or not item:
                    raise _invalid("boolean filter child must be a nonempty object")
                _validate_filter(item)
            continue
        if condition is None:
            raise _invalid("comparison with NULL matches no rows; use $is_null", "UDB_NULL_COMPARISON")
        _validate_operators(condition)


def _matches(row: Mapping[str, Any], filter_: Mapping[str, Any]) -> bool:
    for key, condition in filter_.items():
        name = key.lower()
        if name in ("$and", "$or"):
            results = [_matches(row, item) for item in condition]
            if name == "$and" and not all(results):
                return False
            if name == "$or" and not any(results):
                return False
            continue
        if _column_truth(row.get(key), condition) != _TRUE:
            return False
    return True


def _column_truth(value: Any, condition: Any) -> int:
    """SQL uses three-valued logic: NULL <> x and NOT(NULL = x) stay unknown and cannot
    select a row, including through nested NOT predicates."""
    if not isinstance(condition, dict):
        if condition is None:
            raise _invalid("comparison with NULL matches no rows; use $is_null", "UDB_NULL_COMPARISON")
        if value is None:
            return _UNKNOWN
        return _TRUE if _equal(value, condition) else _FALSE
    unknown = False
    for op, arg in condition.items():
        name = op.lower()
        if name not in ("$is_null", "$not_null", "$not", "$in", "$nin") and (value is None or arg is None):
            unknown = True
            continue
        if name == "$eq":
            ok = _equal(value, arg)
        elif name == "$ne":
            ok = not _equal(value, arg)
        elif name == "$gt":
            ok = _compare(value, arg) > 0
        elif name == "$gte":
            ok = _compare(value, arg) >= 0
        elif name == "$lt":
            ok = _compare(value, arg) < 0
        elif name == "$lte":
            ok = _compare(value, arg) <= 0
        elif name in ("$in", "$nin"):
            items = arg if isinstance(arg, list) else []
            if not items:
                ok = name == "$nin"
            elif value is None:
                unknown = True
                continue
            else:
                found = any(_equal(value, item) for item in items)
                if not found and any(item is None for item in items):
                    unknown = True
                    continue
                ok = found == (name == "$in")
        elif name == "$between":
            bounds = arg if isinstance(arg, list) else []
            if len(bounds) == 2 and (bounds[0] is None or bounds[1] is None):
                unknown = True
                continue
            ok = len(bounds) == 2 and _compare(value, bounds[0]) >= 0 and _compare(value, bounds[1]) <= 0
        elif name == "$is_null":
            ok = (value is None) == (arg if isinstance(arg, bool) else True)
        elif name == "$not_null":
            ok = (value is not None) == (arg if isinstance(arg, bool) else True)
        elif name == "$not":
            inner = _column_truth(value, arg)
            if inner == _UNKNOWN:
                unknown = True
                continue
            ok = inner == _FALSE
        elif name in ("$like", "$ilike"):
            pattern = arg if isinstance(arg, str) else ""
            text = value if isinstance(value, str) else ""
            if name == "$ilike":
                pattern, text = pattern.lower(), text.lower()
            ok = _like(text, pattern)
        else:
            raise _invalid(f"unsupported filter operator {op}", "UDB_UNSUPPORTED_FILTER_OPERATOR")
        if not ok:
            return _FALSE
    return _UNKNOWN if unknown else _TRUE


def _holds(row: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return all(_equal(row.get(column), want) for column, want in expected.items())


def _default_value(expression: str) -> Any:
    base, _, cast = expression.partition("::")
    base = base.strip()
    lowered = base.lower()
    if lowered in ("gen_random_uuid()", "uuid_generate_v4()"):
        return str(uuid.uuid4())
    if lowered in ("now()", "current_timestamp"):
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    if lowered == "null":
        return None
    if lowered in ("true", "false"):
        return lowered == "true"
    if len(base) >= 2 and base[0] == "'" and base[-1] == "'":
        value = base[1:-1].replace("''", "'")
        if cast.strip().lower() in ("json", "jsonb"):
            return json.loads(value)
        return value
    try:
        number = Fraction(base)
    except (ValueError, ZeroDivisionError):
        raise ValueError(f"unsupported SQL default {expression!r}; use start() for database expressions") from None
    return int(number) if number.denominator == 1 else float(number)


def _decode_object(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except ValueError:
        raise ValueError("record_json must be a JSON object") from None
    if not isinstance(value, dict):
        raise ValueError("record_json must be a JSON object")
    return value


# ── entities ─────────────────────────────────────────────────────────────────


@dataclass
class _Unique:
    name: str
    columns: list[str] = field(default_factory=list)
    where: str = ""


def _json_column_document(name: str, text: str) -> Any:
    """A string written to a JSON column is JSON text, as on the broker, and the column holds
    the document it encodes."""
    try:
        document = _loads(text)
    except ValueError as error:
        raise _invalid(
            f"value for JSON column {name} is not valid JSON: send a JSON document, or null to clear it"
        ) from error
    if document is None:
        raise _invalid(f"the text null is not accepted for JSON column {name}: send a real null to clear it")
    return document


class _Entity:
    def __init__(self, message: Any):
        self.message_class = message if isinstance(message, type) else type(message)
        descriptor = self.message_class.DESCRIPTOR
        self.message_type = message_type(descriptor)
        self.primary_key = primary_keys(descriptor)
        self.tenant_column = tenant_column(descriptor)
        self.project_column = project_column(descriptor)
        self.columns: dict[str, Any] = {}
        self.options: dict[str, Any] = {}
        self.aliases: dict[str, str] = {}
        self.masked: dict[str, bool] = {}
        self.omit_implicit: dict[str, bool] = {}
        self.pii_scope: dict[str, bool] = {}
        self.unique: list[_Unique] = []
        self.soft_delete = ""
        self.schema_error = ""
        table = table_options(descriptor)
        self.table_name = (table.table_name if table is not None else "") or descriptor.name
        for fd in descriptor.fields:
            column = column_options(fd)
            if column is None:
                continue
            name = fd.name
            self.columns[name] = fd
            self.options[name] = column
            self.aliases[name.lower()] = name
            if column.column_name:
                self.aliases[column.column_name.lower()] = name
            self.masked[name] = is_masked(fd)
            self.omit_implicit[name] = is_pii(fd) or is_encrypted(fd)
            self.pii_scope[name] = self.omit_implicit[name] or column.encrypted
        for name, column in self.options.items():
            if column.unique:
                self.unique.append(_Unique(f"{self.table_name}_{name}_key", [name]))
            if column.HasField("index") and column.index.unique:
                self._add_unique(column.index, name)
        if table is not None:
            for index in table.indexes:
                if index.unique:
                    self._add_unique(index, "")
            if table.soft_delete:
                column_name = table.soft_delete_column or "deleted_at"
                resolved = self.aliases.get(column_name.lower())
                if resolved is None:
                    self.schema_error = "soft_delete_column is not a declared column"
                self.soft_delete = resolved or column_name

    def _add_unique(self, index: Any, fallback: str) -> None:
        columns = list(index.columns) or list(index.composite_fields)
        if not columns and fallback:
            columns = [fallback]
        key = _Unique(index.index_name, where=index.where_clause.strip())
        for column in columns:
            name = self.aliases.get(column.lower())
            if name is None:
                self.schema_error = f"unique index refers to an unknown column: {column}"
                return
            key.columns.append(name)
        if not key.columns:
            self.schema_error = "unique index has no columns"
            return
        key.name = key.name or f"{self.table_name}_{'_'.join(key.columns)}_key"
        # A fake must never silently drop a partial uniqueness predicate. The common
        # soft-delete predicates are supported; other SQL needs start().
        if key.where:
            try:
                self.in_unique(key, {})
            except ValueError as error:
                self.schema_error = str(error)
                return
        self.unique.append(key)

    def in_unique(self, key: _Unique, row: Mapping[str, Any]) -> bool:
        if not key.where:
            return True
        parts = key.where.split()
        if (
            len(parts) in (3, 4)
            and parts[1].upper() == "IS"
            and parts[-1].upper() == "NULL"
            and (len(parts) == 3 or parts[2].upper() == "NOT")
        ):
            name = self.aliases.get(parts[0].strip('"'))
            if name is not None:
                return (row.get(name) is None) == (len(parts) == 3)
        raise ValueError(f"udbtest: unsupported unique-index predicate {key.where!r}; use start() for SQL expressions")

    def normalize(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if self.schema_error:
            raise _refuse(grpc.StatusCode.FAILED_PRECONDITION, "", self.schema_error)
        out: dict[str, Any] = {}
        for column, value in record.items():
            name = self.aliases.get(column.lower())
            if name is None:
                raise _invalid(f"unknown column {column}")
            if name in out:
                raise _invalid(f"column supplied through two aliases: {name}")
            if self.masked[name] and value in (_MASK, _REDACTED):
                reason = "UDB_REDACTED_VALUE_WRITE"
                raise _Refusal(
                    grpc.StatusCode.INVALID_ARGUMENT, reason, "cannot write a redacted placeholder",
                    error_pb2.ErrorDetail(reason=reason, kind=error_pb2.ERROR_KIND_REDACTED, column=name),
                )
            if (
                isinstance(value, float)
                and _integer_column(self.options[name])
                and (abs(value) > 2**53 or not value.is_integer())
            ):
                raise _invalid(f"integer column requires an exact number or decimal string: {name}")
            if value is None:
                out[name] = None
                continue
            if isinstance(value, str) and _is_json(self.options[name]):
                out[name] = _json_column_document(name, value)
                continue
            # Reuse the SDK codec for coercion, enums, timestamps, nested JSON and
            # exact integer bounds. No second lossy JSON-to-protobuf implementation.
            message = self.message_class()
            try:
                decode_field(message, name, value)
                encoded, present = encode_field(message, name)
            except ValueError as error:
                raise _invalid(str(error)) from error
            if present:
                out[name] = copy.deepcopy(encoded)
        return out

    def defaults(self, record: dict[str, Any]) -> None:
        for name, column in self.options.items():
            if name in record:
                continue
            expression = column.default_value.strip()
            if expression:
                try:
                    record[name] = _default_value(expression)
                except ValueError as error:
                    raise _refuse(
                        grpc.StatusCode.FAILED_PRECONDITION, "", f"udbtest: default for {name}: {error}"
                    ) from error

    def key_of(self, record: Mapping[str, Any]) -> str:
        if not self.primary_key:
            raise _refuse(grpc.StatusCode.FAILED_PRECONDITION, "", f"{self.message_type} declares no primary key")
        parts = []
        for column in self.primary_key:
            value = record.get(column)
            if value is None or value == "":
                raise _column_refusal(column)
            parts.append(value)
        return json.dumps(parts, separators=(",", ":"))

    def live(self, row: Mapping[str, Any]) -> bool:
        return not self.soft_delete or row.get(self.soft_delete) is None

    def check_tenant(self, filter_: Mapping[str, Any], tenant: str) -> None:
        if not self.tenant_column:
            if "tenant_id" in filter_:
                raise _invalid("the entity has no tenant column", "UDB_TABLE_NOT_TENANT_SCOPED")
            return
        value = filter_.get(self.tenant_column)
        if isinstance(value, dict) and len(value) == 1:
            value = value.get("$eq")
        if isinstance(value, str) and value != tenant:
            raise _refuse(
                grpc.StatusCode.PERMISSION_DENIED, "UDB_TENANT_MISMATCH",
                "the request names a different tenant than the caller's verified tenant",
            )

    def query_filter(self, filter_: Mapping[str, Any]) -> dict[str, Any]:
        """Resolve every column at plan time, including empty tables and nested boolean
        branches. Unknown columns must never look like a successful zero-row query."""
        _validate_filter(filter_)
        out: dict[str, Any] = {}
        for field_name, condition in filter_.items():
            name = field_name.strip().lower()
            if name in ("$and", "$or"):
                if name in out:
                    raise _invalid(f"boolean filter supplied through two aliases: {name}")
                out[name] = [self.query_filter(child) for child in condition]
                continue
            if name == "tenant_id" and not self.tenant_column:
                raise _invalid("the entity has no tenant column", "UDB_TABLE_NOT_TENANT_SCOPED")
            column = self.aliases.get(name)
            if column is None:
                raise _invalid(f"unknown filter column {field_name}")
            if column in out:
                raise _invalid(f"filter column supplied through two aliases: {column}")
            out[column] = _canonical_operators(condition)
        return out

    def expected_columns(self, values: Mapping[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for field_name, value in values.items():
            column = self.aliases.get(field_name.strip().lower())
            if column is None:
                raise _invalid(f"unknown expected column {field_name}")
            if column in out:
                raise _invalid(f"expected column supplied through two aliases: {column}")
            # A JSON column holds the document its text encodes, and the broker
            # compares `expected` as that document: "[]" must equal the [] it stored.
            if isinstance(value, str) and _is_json(self.options[column]):
                out[column] = _json_column_document(column, value)
                continue
            out[column] = copy.deepcopy(value)
        return out

    def conflict_fields(self, fields: Sequence[str]) -> None:
        if not fields:
            return
        seen: set[str] = set()
        for field_name in fields:
            column = self.aliases.get(field_name.strip().lower())
            if column is None or column in seen:
                raise _invalid(f"unknown or duplicate conflict column {field_name}")
            seen.add(column)
        if seen != set(self.primary_key):
            raise _refuse(
                grpc.StatusCode.FAILED_PRECONDITION, "",
                "udbtest: alternate Upsert conflict_fields require start(); they are never ignored",
            )

    def update_columns(self, changes: Mapping[str, Any], increments: Sequence[Any]) -> None:
        """Follow the broker's update admission before row matching. A primary key,
        tenant or project is immutable even when a caller supplies its current value."""
        if not changes and not increments:
            raise _invalid("update requires at least one change or increment")

        def resolve(field_name: str) -> str:
            name = self.aliases.get(field_name.strip().lower())
            if name is None:
                raise _invalid(f"unknown update column {field_name}")
            if name in (self.tenant_column, self.project_column) or name in self.primary_key:
                raise _invalid(
                    f"column {name} is immutable through Update (primary key / tenant / project); "
                    "use Upsert for identity changes"
                )
            return name

        changed = {resolve(field_name) for field_name in changes}
        for increment in increments:
            name = resolve(increment.column)
            if name in changed:
                raise _invalid(f"column {name} appears in both changes and increments")

    def validate_cas_key(self, filter_: Mapping[str, Any]) -> None:
        """CAS must be pinned to an entire declared key, even when another filter
        happens to match exactly one row today."""
        pins: set[str] = set()

        def collect(part: Mapping[str, Any]) -> None:
            for name, value in part.items():
                if name.lower() == "$and":
                    for item in value:
                        collect(item)
                    continue
                if isinstance(value, dict):
                    value = next((v for op, v in value.items() if op.lower() == "$eq"), None)
                if value is not None and not name.startswith("$"):
                    pins.add(name)

        collect(filter_)
        keys = [self.primary_key] + [unique.columns for unique in self.unique if not unique.where]
        if any(key and all(name in pins for name in key) for key in keys):
            return
        raise _refuse(
            grpc.StatusCode.FAILED_PRECONDITION, "UDB_CAS_KEY_NOT_PK",
            "CAS requires equality on every column of a primary or unique key",
        )

    def read_columns(self, call: "_Call", fields: Sequence[str]) -> set[str]:
        """Follow the planner's implicit column list and the served export-control gate.
        Even a PII reader must name a PII column explicitly; implicit * omits it."""
        selected: set[str] = set()
        can_read = _has_scope(call.identity, "udb:pii:read")
        purposes = call.md.get("x-purpose", [])
        if len(purposes) == 1 and purposes[0] in ("export", "verification", "audit"):
            can_read = True
        if not fields:
            for name in self.columns:
                if self.omit_implicit[name]:
                    continue
                if self.pii_scope[name] and not can_read:
                    raise _missing_scope("udb:pii:read")
                selected.add(name)
            return selected
        for field_name in fields:
            name = self.aliases.get(field_name.strip().lower())
            if name is None:
                raise _invalid(f"unknown selected column {field_name}")
            if self.pii_scope[name] and not can_read:
                raise _missing_scope("udb:pii:read")
            selected.add(name)
        return selected

    def wire_value(self, name: str, value: Any) -> Any:
        """The broker's integer wire shape: an exact JSON number through 2^53, then a
        decimal string. Storage keeps the exact value on either side."""
        if value is None or self.columns[name].type not in _INT64_TYPES:
            return value

        def convert(item: Any) -> Any:
            try:
                number = int(str(item))
            except ValueError:
                return item
            return str(number) if abs(number) > 2**53 else number

        if isinstance(value, list) and is_repeated(self.columns[name]):
            return [convert(item) for item in value]
        return convert(value)

    def bind_record_scope(self, record: dict[str, Any], identity: "Identity") -> None:
        if self.tenant_column:
            given = record.get(self.tenant_column)
            if isinstance(given, str) and given and given != identity.tenant_id:
                raise _refuse(
                    grpc.StatusCode.PERMISSION_DENIED, "UDB_TENANT_MISMATCH",
                    "the record names a different tenant than the caller's",
                )
            record[self.tenant_column] = identity.tenant_id
        if self.project_column and identity.project_id:
            given = record.get(self.project_column)
            if given is not None and given != identity.project_id:
                raise _invalid("record project_id must match RequestContext.project_id")
            record[self.project_column] = identity.project_id

    def visible_to(self, row: Mapping[str, Any], identity: "Identity") -> bool:
        return (
            not self.project_column
            or not identity.project_id
            or row.get(self.project_column) == identity.project_id
        )

    def same_record_scope(self, existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> bool:
        for column in (self.tenant_column, self.project_column):
            if column and column in incoming and not _equal(existing.get(column), incoming[column]):
                return False
        return True

    def scoped_write_filter(self, filter_: Mapping[str, Any], identity: "Identity") -> dict[str, Any]:
        out = copy.deepcopy(dict(filter_))
        scope: dict[str, str] = {}
        if self.tenant_column:
            scope[self.tenant_column] = identity.tenant_id
        if self.project_column and identity.project_id:
            scope[self.project_column] = identity.project_id
        referenced: set[str] = set()

        def visit(part: Mapping[str, Any]) -> None:
            for field_name, value in part.items():
                if field_name.startswith("$"):
                    if isinstance(value, list):
                        for child in value:
                            if isinstance(child, dict):
                                visit(child)
                    elif isinstance(value, dict):
                        visit(value)
                else:
                    referenced.add(field_name)

        visit(out)
        if any(column not in scope for column in referenced):
            for column, value in scope.items():
                if column not in referenced:
                    out[column] = value
        return out

    def revision_filter_key(self, identity: "Identity", filter_: Mapping[str, Any]) -> str:
        """Opaque revisions address only primary keys, unlike field-map CAS which also
        accepts complete unique keys."""
        scoped = self.scoped_write_filter(filter_, identity)
        key: dict[str, Any] = {}
        for column in self.primary_key:
            value = scoped.get(column)
            if isinstance(value, dict):
                value = value.get("$eq") if len(value) == 1 else None
            if value is None:
                raise _violation(
                    grpc.StatusCode.FAILED_PRECONDITION, "UDB_CAS_KEY_NOT_PK",
                    "conditional mutation requires an equality filter on every primary-key column",
                    error_pb2.ERROR_KIND_VALIDATION, column, "primary-key column must be pinned by equality",
                )
            key[column] = value
        return self.key_of(self.normalize(key))

    def mutation_inputs(self, identity: "Identity", request: Any) -> dict[str, Any]:
        inputs: dict[str, Any] = {"expected": None}
        filter_: dict[str, Any] | None = None
        if isinstance(request, types_pb2.UpsertRequest):
            try:
                record = _record_of(request)
            except ValueError as error:
                raise _invalid(str(error)) from error
            record = self.normalize(record)
            self.bind_record_scope(record, identity)
            inputs["record"], inputs["conflict_fields"] = record, list(request.conflict_fields)
        else:
            if request.expected_revision.strip():
                inputs["expected_revision"] = request.expected_revision.strip()
            if request.require_affected:
                inputs["require_affected"] = request.require_affected
            if isinstance(request, types_pb2.UpdateRequest):
                inputs["changes"] = self.normalize(_struct_map(request.changes))
                inputs["increments"] = [[item.column, item.delta] for item in request.increments]
            filter_ = _struct_map(request.filter)
        if len(request.expected.fields) > 0:
            inputs["expected"] = _struct_map(request.expected)
        if filter_ is not None:
            inputs["filter"] = self.scoped_write_filter(self.query_filter(filter_), identity)
        return inputs


def _integer_column(column: Any) -> bool:
    return column.sql_type.strip().upper() in _INTEGER_SQL


def _canonical_operators(value: Any) -> Any:
    if not isinstance(value, dict):
        return copy.deepcopy(value)
    out: dict[str, Any] = {}
    for op, arg in value.items():
        name = op.lower()
        if name in out:
            raise _invalid(f"filter operator supplied through two aliases: {name}")
        out[name] = _canonical_operators(arg) if name == "$not" else copy.deepcopy(arg)
    return out


def _record_of(request: Any) -> dict[str, Any]:
    # The Python client sends an empty payload Struct alongside record_json.
    if request.HasField("payload") and len(request.payload.fields) > 0:
        return _struct_map(request.payload)
    return _decode_object(request.record_json)


def _protobuf_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _protobuf_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_protobuf_json(item) for item in value]
    if isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    return value


def _increment(value: Any, delta: float, integer: bool) -> Any:
    if value is None:
        return None  # SQL NULL + delta is NULL.
    current = _numeric(value)
    if current is None:
        current = _text_number(value)
    change = _numeric(delta)
    if current is None or change is None:
        raise _invalid("increment requires a finite numeric column and delta")
    result = current + change
    if integer:
        if result.denominator != 1 or abs(result) >= 2**63:
            raise _invalid("integer increment is fractional or out of range")
        return int(result)
    return float(result)


# ── identities, faults and policy ────────────────────────────────────────────


@dataclass
class Identity:
    """A broker-verified fake identity, provisioned with an API key. Request headers
    cannot add scopes or roles, or change its tenant, project or owner."""

    tenant_id: str
    project_id: str = ""
    subject: str = ""
    user_id: str = ""
    service_identity: str = ""
    scopes: Sequence[str] = ()
    roles: Sequence[str] = ()


@dataclass
class Fault:
    """A bounded number of RPC failures, injected before any mutation applies."""

    code: grpc.StatusCode
    #: A bare RPC name or full path; empty matches any RPC. ``Authenticate`` failures
    #: exercise API-key re-exchange through the unmodified SDK.
    method: str = ""
    tenant_id: str = ""
    reason: str = ""
    #: How many calls fail; 0 means one.
    times: int = 0
    #: Seconds, surfaced as the broker's ``retry_after_ms``.
    retry_after: float = 0.0


@dataclass
class Policy:
    """One policy rule, with the same fields ``udb policy apply`` reads. A deny ignores
    ``required_scopes``, as the broker does, so dropping a scope cannot cancel a deny."""

    id: str = ""
    priority: int = 0
    enabled: bool | None = None
    effect: str = ""
    tenant: str = ""
    project: str = ""
    subject: str = ""
    role: str = ""
    action: str = ""
    resource: str = ""
    purpose: str = ""
    relationship: str = ""
    conditions: Mapping[str, str] = field(default_factory=dict)
    required_scopes: Sequence[str] = ()


@dataclass
class Event:
    """One outbox event a committed transaction emitted."""

    topic: str
    partition_key: str
    envelope: dict[str, Any]


def _has_scope(identity: Identity, scope: str) -> bool:
    if not scope.strip():
        return True
    return any(granted in (scope, "*", "udb:*") for granted in identity.scopes)


def _wildcard(pattern: str, value: str) -> bool:
    pattern = pattern.strip()
    return pattern in ("", "*", value)


def _policy_pattern(pattern: str, value: str) -> bool:
    pattern = pattern.strip()
    if _wildcard(pattern, value):
        return True
    if pattern.endswith(".*"):
        prefix = pattern[:-2]
        return value == prefix or value.startswith(prefix + ".")
    return False


def _consumer_name(name: str) -> str:
    name = name.strip()
    if not 1 <= len(name) <= 120:
        raise _invalid("invalid consumer_name length")
    if not _CONSUMER_CHARS.match(name):
        raise _invalid("invalid consumer_name character")
    return name


def _consumer_owner(identity: Identity) -> str:
    if identity.service_identity:
        return f"service:{identity.service_identity}"
    if identity.user_id:
        return f"user:{identity.user_id}"
    raise _refuse(grpc.StatusCode.UNAUTHENTICATED, "", "named CDC consumer requires a verified user or service identity")


def _cursor_key(identity: Identity, consumer: str, pattern: str) -> str:
    return "\x00".join((identity.tenant_id, identity.project_id, consumer, pattern))


def _topic_pattern(pattern: str) -> str:
    return pattern.strip() or "*"


def _topic_matches(pattern: str, topic: str) -> bool:
    if pattern in ("*", topic):
        return True
    return pattern.endswith("*") and topic.startswith(pattern[:-1])


def _event_in_scope(event: Event, identity: Identity, pattern: str) -> bool:
    project = event.envelope.get("project_id")
    return (
        event.envelope.get("tenant_id") == identity.tenant_id
        and (project if isinstance(project, str) else "") == identity.project_id
        and _topic_matches(pattern, event.topic)
    )


class _Call:
    """What the handlers know about the RPC in flight."""

    def __init__(self, context: grpc.ServicerContext):
        self.context = context
        self.md: dict[str, list[str]] = {}
        for key, value in context.invocation_metadata() or ():
            self.md.setdefault(key, []).append(value)
        self.identity = Identity(tenant_id="")
        self.expires = 0.0


# ── the fake ─────────────────────────────────────────────────────────────────


class Fake:
    """The in-memory broker. Use it as a context manager, or call :meth:`close`."""

    def __init__(self, *, default_allow: bool = True):
        self._lock = threading.RLock()
        self._entities: dict[str, _Entity] = {}
        self._rows: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        self._identities: dict[str, Identity] = {}
        self._tokens: dict[str, tuple[Identity, float]] = {}
        self._token_lifetime = 3600.0
        self._faults: list[Fault] = []
        self._events: list[Event] = []
        self._cursors: dict[str, dict[str, Any]] = {}
        self._dlq: dict[str, dict[str, Any]] = {}
        self._receipts: dict[tuple[str, ...], tuple[str, types_pb2.MutationResponse]] = {}
        self._revisions: dict[tuple[str, ...], int] = {}
        self._emitted = 0
        self._policies: list[Policy] = []
        self._default_allow = default_allow
        self._tuples: list[tuple[str, str, str, str, str]] = []
        self._rate: dict[str, Any] = {"limit": 0, "window": 0.0, "per_principal": False, "buckets": {}}
        self._clients: list[UdbProject] = []
        self._server: grpc.Server | None = None
        self.target = ""

    # Lifecycle

    def serve(self) -> "Fake":
        unary = lambda method, fn, request, response: grpc.unary_unary_rpc_method_handler(  # noqa: E731
            self._unary(method, fn), request_deserializer=request.FromString, response_serializer=response.SerializeToString
        )
        t = types_pb2
        broker = {
            "Select": unary("Select", self._select, t.SelectRequest, t.RecordSet),
            "Upsert": unary("Upsert", self._upsert, t.UpsertRequest, t.MutationResponse),
            "Insert": unary("Insert", self._insert, t.UpsertRequest, t.MutationResponse),
            "Update": unary("Update", self._update, t.UpdateRequest, t.MutationResponse),
            "Delete": unary("Delete", self._delete, t.DeleteRequest, t.MutationResponse),
            "AckCdcEvents": unary("AckCdcEvents", self._ack, t.AckCdcEventsRequest, t.AckCdcEventsResponse),
            "NackCdcEvents": unary("NackCdcEvents", self._nack, t.NackCdcEventsRequest, t.AckCdcEventsResponse),
            "ListDlqEvents": unary("ListDlqEvents", self._list_dlq, t.DlqListRequest, t.DlqListResponse),
            "GetDlqEvent": unary("GetDlqEvent", self._get_dlq, t.DlqEventRequest, t.DlqEventResponse),
            "ReplayDlqEvent": unary("ReplayDlqEvent", self._replay_dlq, t.DlqActionRequest, t.MutationResponse),
            "DismissDlqEvent": unary("DismissDlqEvent", self._dismiss_dlq, t.DlqActionRequest, t.MutationResponse),
            "QuarantineDlqEvent": unary("QuarantineDlqEvent", self._quarantine_dlq, t.DlqActionRequest, t.MutationResponse),
            "BeginTx": grpc.stream_stream_rpc_method_handler(
                self._begin_tx, request_deserializer=t.Mutation.FromString, response_serializer=t.TxStatus.SerializeToString
            ),
            "PublishCDC": grpc.unary_stream_rpc_method_handler(
                self._publish_cdc,
                request_deserializer=t.CDCSubscriptionRequest.FromString,
                response_serializer=udb_events_pb2.CDCEnvelope.SerializeToString,
            ),
        }
        authn = {
            "Authenticate": unary("Authenticate", self._authenticate, authn_pb2.AuthnRequest, authn_pb2.AuthnResponse),
        }
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=64))
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler("udb.services.v1.DataBroker", broker),
                grpc.method_handlers_generic_handler("udb.core.authn.services.v1.AuthnService", authn),
            )
        )
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        self._server = server
        self.target = f"127.0.0.1:{port}"
        return self

    def close(self) -> None:
        for client in self._clients:
            client.close()
        self._clients.clear()
        with self._lock:
            server, self._server = self._server, None
        if server is not None:
            server.stop(0)

    def __enter__(self) -> "Fake":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # Provisioning

    def register(self, message: Any) -> None:
        """Add an entity message type (class or instance) to the fake. Its udb column
        annotations define keys and the tenant column."""
        entity = _Entity(message)
        with self._lock:
            self._entities[entity.message_type] = entity

    def add_api_key(self, key: str, tenant: str) -> None:
        """Make ``key`` exchangeable for a bearer of ``tenant``."""
        self.add_identity_api_key(
            key,
            Identity(
                tenant_id=tenant,
                service_identity=f"udbtest-service-{tenant}",
                scopes=("udb:read", "udb:write", "udb:admin", "udb:cdc:read", "udb:cdc:subscribe"),
            ),
        )

    def add_identity_api_key(self, key: str, identity: Identity) -> None:
        """Provision immutable verified identity attributes for ``key``."""
        subject = identity.subject or identity.user_id or identity.service_identity
        provisioned = replace(identity, subject=subject, scopes=tuple(identity.scopes), roles=tuple(identity.roles))
        with self._lock:
            self._identities[key] = provisioned

    def client(self, tenant: str) -> UdbProject:
        """Connect a client for ``tenant`` with an exchanged API key, as a service
        would. :meth:`close` closes it."""
        key = f"udbtest-key-{tenant}"
        self.add_api_key(key, tenant)
        return self.client_for_key(key)

    def client_for_key(self, key: str) -> UdbProject:
        """Connect with a provisioned key without replacing its identity."""
        with self._lock:
            identity = self._identities.get(key)
        if identity is None:
            raise ValueError("udbtest: key is not provisioned")
        client = UdbProject(
            UdbConfig(
                target=self.target,
                tenant_id=identity.tenant_id,
                project_id=identity.project_id,
                purpose="test",
                api_key=key,
            )
        )
        self._clients.append(client)
        return client

    def async_client(self, tenant: str) -> UdbAsyncProject:
        """An async client for ``tenant``; close it from its event loop."""
        key = f"udbtest-key-{tenant}"
        self.add_api_key(key, tenant)
        with self._lock:
            identity = self._identities[key]
        token = self._authenticate_key(key).access_token
        return UdbAsyncProject(
            UdbConfig(
                target=self.target,
                tenant_id=identity.tenant_id,
                project_id=identity.project_id,
                purpose="test",
                bearer_token=token,
            )
        )

    def rows(self, message_type_: str, tenant: str) -> list[dict[str, Any]]:
        """A copy of every stored row of a message type for ``tenant``, for assertions."""
        with self._lock:
            rows = [copy.deepcopy(row) for row in self._rows.get(message_type_, {}).get(tenant, {}).values()]
        return sorted(rows, key=_json_text)

    def events(self) -> list[Event]:
        """The outbox events committed so far, in commit order."""
        with self._lock:
            return [Event(e.topic, e.partition_key, copy.deepcopy(e.envelope)) for e in self._events]

    def emit(self, tenant: str, topic: str, partition_key: str, payload: Mapping[str, Any]) -> str:
        """Append an event as if a transaction of ``tenant`` had committed it, for tests
        that only exercise a consumer. Returns its event id."""
        with self._lock:
            self._emitted += 1
            event_id = f"0190f1b2-0000-4000-8000-{self._emitted:012x}"
            self._events.append(
                Event(
                    topic,
                    partition_key,
                    {
                        "event_id": event_id, "event_type": topic, "tenant_id": tenant,
                        "document_id": partition_key, "correlation_id": event_id,
                        "envelope_version": EVENT_ENVELOPE_VERSION,
                        "payload": copy.deepcopy(dict(payload)), "project_id": "",
                    },
                )
            )
        return event_id

    def inject_fault(self, fault: Fault) -> None:
        """Fail the next matching calls, before any mutation applies."""
        if fault.code == _STATUS_OK or fault.times < 0 or fault.retry_after < 0:
            raise ValueError("udbtest: fault needs a failure code and non-negative count/delay")
        method = fault.method.rsplit("/", 1)[-1]
        with self._lock:
            self._faults.append(replace(fault, method=method, times=fault.times or 1))

    def set_token_lifetime(self, seconds: float) -> None:
        """Tune subsequently-issued bearers. Existing bearers keep their expiry."""
        if seconds < 1:
            raise ValueError("udbtest: token lifetime must be at least one second")
        with self._lock:
            self._token_lifetime = float(seconds)

    def set_rate_limit(self, limit: int, window: float, *, per_principal: bool = False) -> None:
        """The broker's fixed-window bucket by tenant and operation, optionally per
        principal. A zero limit disables it."""
        if limit < 0 or (limit > 0 and window <= 0):
            raise ValueError("udbtest: invalid rate limit")
        with self._lock:
            self._rate = {"limit": limit, "window": float(window), "per_principal": per_principal, "buckets": {}}

    def set_policies(self, policies: Iterable[Policy]) -> None:
        """Judge requests by these rules. With none, requests are judged by the
        ``default_allow`` this fake was created with."""
        with self._lock:
            self._policies = list(policies)

    def add_relationship(self, subject: str, relation: str, object_: str, tenant: str, project: str) -> None:
        """Provision a subject/relation/object tuple the policy rules consult."""
        with self._lock:
            self._tuples.append((subject, relation, object_, tenant, project))

    # Handler plumbing

    def _abort(self, context: grpc.ServicerContext, refusal: _Refusal) -> None:
        if refusal.reason or refusal.detail is not None:
            detail = refusal.detail or error_pb2.ErrorDetail(
                reason=refusal.reason, kind=_error_kind(refusal.code, refusal.reason)
            )
            context.set_trailing_metadata(((_DETAIL_TRAILER, detail.SerializeToString()),))
        context.abort(refusal.code, refusal.message)

    def _unary(self, method: str, fn: Callable[[_Call, Any], Any]) -> Callable[[Any, grpc.ServicerContext], Any]:
        def handler(request: Any, context: grpc.ServicerContext) -> Any:
            call = _Call(context)
            try:
                self._before_rpc(call, method, request)
                return fn(call, request)
            except _Refusal as refusal:
                self._abort(context, refusal)

        return handler

    def _before_rpc(self, call: _Call, method: str, request: Any) -> None:
        if method != "Authenticate":
            authorization = call.md.get("authorization", [])
            if len(authorization) != 1 or not authorization[0].startswith("Bearer "):
                raise _refuse(grpc.StatusCode.UNAUTHENTICATED, "", "a verified bearer is required")
            with self._lock:
                token = self._tokens.get(authorization[0][len("Bearer "):])
            if token is None or time.time() >= token[1]:
                raise _refuse(grpc.StatusCode.UNAUTHENTICATED, "", "bearer is unknown or expired")
            identity, call.expires = token
            for key in ("x-tenant-id", "x-udb-tenant-id"):
                supplied = call.md.get(key, [])
                if len(supplied) > 1 or (len(supplied) == 1 and supplied[0] != identity.tenant_id):
                    raise _refuse(
                        grpc.StatusCode.PERMISSION_DENIED, "UDB_TENANT_MISMATCH",
                        "tenant header differs from verified tenant",
                    )
            for key in ("x-project-id", "x-udb-project-id"):
                supplied = call.md.get(key, [])
                if len(supplied) > 1 or (len(supplied) == 1 and supplied[0] != identity.project_id):
                    raise _refuse(
                        grpc.StatusCode.PERMISSION_DENIED, "UDB_POLICY_DENIED",
                        "project header differs from verified project",
                    )
            call.identity = identity
            if request is not None and hasattr(request, "context"):
                self._bind_body(call, request.context)
            if method in _DLQ_METHODS and not _has_scope(identity, "udb:admin"):
                raise _missing_scope("udb:admin")
            if request is not None and hasattr(request, "message_type"):
                self._authorize(call, request.message_type, "Upsert" if method == "Insert" else method)
        self._admit(call, method, request)

    def _bind_body(self, call: _Call, body: types_pb2.RequestContext) -> None:
        if body.tenant_id and body.tenant_id != call.identity.tenant_id:
            raise _refuse(grpc.StatusCode.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "body tenant differs from verified tenant")
        if body.project_id and body.project_id != call.identity.project_id:
            raise _refuse(grpc.StatusCode.PERMISSION_DENIED, "UDB_POLICY_DENIED", "body project differs from verified project")

    def _allows(self, identity: Identity, resource: str, action: str, purpose: str) -> bool:
        allowed = not self._policies and self._default_allow
        for policy in self._policies:
            if policy.enabled is False:
                continue
            if not (
                _wildcard(policy.tenant, identity.tenant_id)
                and _wildcard(policy.project, identity.project_id)
                and _policy_pattern(policy.action, action)
                and _policy_pattern(policy.resource, resource)
                and _wildcard(policy.purpose, purpose)
            ):
                continue
            subject = policy.subject.strip()
            if subject not in ("", "*", identity.subject, identity.user_id, identity.service_identity):
                continue
            role = policy.role.strip()
            if role not in ("", "*") and role not in identity.roles:
                continue
            # The data-plane gate uses no caller-asserted attributes.
            if policy.conditions:
                continue
            if policy.relationship and not any(
                relation == policy.relationship
                and object_ == resource
                and subject_ in (identity.subject, identity.user_id, identity.service_identity)
                and _wildcard(tenant, identity.tenant_id)
                and _wildcard(project, identity.project_id)
                for subject_, relation, object_, tenant, project in self._tuples
            ):
                continue
            if policy.effect == "deny":
                return False
            if all(_has_scope(identity, scope) for scope in policy.required_scopes):
                allowed = True
        return allowed

    def _authorize(self, call: _Call, resource: str, action: str) -> None:
        purposes = call.md.get("x-purpose", [])
        purpose = purposes[0] if purposes else ""
        if not purpose.strip():
            raise _refuse(grpc.StatusCode.PERMISSION_DENIED, "UDB_POLICY_DENIED", "purpose is required")
        with self._lock:
            allowed = self._allows(call.identity, resource, action, purpose)
        if not allowed:
            reason = "UDB_POLICY_DENIED"
            raise _Refusal(
                grpc.StatusCode.PERMISSION_DENIED, reason, "no matching allow or an explicit deny policy",
                error_pb2.ErrorDetail(
                    reason=reason, kind=error_pb2.ERROR_KIND_POLICY,
                    missing={"rule": f"{action} {resource}", "purpose": purpose},
                ),
            )

    def _admit(self, call: _Call, method: str, request: Any) -> None:
        identity = call.identity
        with self._lock:
            if method == "Authenticate" and request is not None:
                identity = self._identities.get(request.api_key, identity)
            for fault in self._faults:
                if (
                    fault.times == 0
                    or (fault.method and fault.method != method)
                    or (fault.tenant_id and fault.tenant_id != identity.tenant_id)
                ):
                    continue
                fault.times -= 1
                reason = fault.reason or {
                    grpc.StatusCode.UNAVAILABLE: "UDB_BACKEND_UNAVAILABLE",
                    grpc.StatusCode.RESOURCE_EXHAUSTED: "UDB_RATE_LIMITED",
                    grpc.StatusCode.ABORTED: "UDB_CAS_CONFLICT",
                }.get(fault.code, "")
                raise _Refusal(
                    fault.code, reason, f"injected {method} failure",
                    error_pb2.ErrorDetail(
                        reason=reason,
                        kind=_error_kind(fault.code, reason),
                        retryable=fault.code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.RESOURCE_EXHAUSTED),
                        retry_after_ms=int(fault.retry_after * 1000),
                    ),
                )
            rate = self._rate
            if method == "Authenticate" or rate["limit"] == 0:
                return
            key = f"{identity.tenant_id}\x00{method}"
            if rate["per_principal"]:
                key += f"\x00{identity.subject}"
            now = time.time()
            window = rate["window"]
            bucket = rate["buckets"].get(key)
            if bucket is None or now - bucket[0] >= window:
                bucket = [math.floor(now / window) * window, 0]
            bucket[1] += 1
            rate["buckets"][key] = bucket
            if bucket[1] <= rate["limit"]:
                return
            delay = max(0.001, window - (now - bucket[0]))
            reason = "UDB_RATE_LIMITED"
            raise _Refusal(
                grpc.StatusCode.RESOURCE_EXHAUSTED, reason, "verified caller exceeded the configured rate limit",
                error_pb2.ErrorDetail(
                    reason=reason, kind=error_pb2.ERROR_KIND_RATE_LIMITED, retryable=True,
                    retry_after_ms=int(delay * 1000), operation=method,
                    missing={"bucket": key, "limit": str(rate["limit"]),
                             "window_ms": str(int(window * 1000)), "principal": identity.subject},
                ),
            )

    def _entity(self, name: str) -> _Entity:
        entity = self._entities.get(name)
        if entity is None:
            raise _invalid(f"unknown message_type {name}: the fake serves no such entity", "UDB_UNKNOWN_MESSAGE_TYPE")
        if entity.schema_error:
            raise _refuse(grpc.StatusCode.FAILED_PRECONDITION, "", entity.schema_error)
        return entity

    def _table(self, name: str, tenant: str) -> dict[str, dict[str, Any]]:
        return self._rows.setdefault(name, {}).setdefault(tenant, {})

    # Authn

    def _authenticate_key(self, key: str) -> authn_pb2.AuthnResponse:
        with self._lock:
            identity = self._identities.get(key)
            if identity is None or not key or not identity.tenant_id:
                raise _refuse(grpc.StatusCode.UNAUTHENTICATED, "", "unknown API key")
            token = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")
            expires = int(time.time() + self._token_lifetime)
            self._tokens[token] = (identity, float(expires))
        return authn_pb2.AuthnResponse(
            access_token=token,
            expires_at_unix=expires,
            principal=authn_pb2.Principal(
                tenant_id=identity.tenant_id,
                project_id=identity.project_id,
                principal_id=identity.subject,
                subject=identity.subject,
                user_id=identity.user_id,
                service_identity=identity.service_identity,
                scopes=list(identity.scopes),
                roles=list(identity.roles),
                expires_at_unix=expires,
            ),
        )

    def _authenticate(self, call: _Call, request: Any) -> authn_pb2.AuthnResponse:
        return self._authenticate_key(request.api_key)

    # Data verbs

    def _select(self, call: _Call, request: types_pb2.SelectRequest) -> types_pb2.RecordSet:
        with self._lock:
            entity = self._entity(request.message_type)
            identity = call.identity
            filter_ = entity.query_filter(_struct_map(request.filter))
            entity.check_tenant(filter_, identity.tenant_id)
            selected = entity.read_columns(call, list(request.fields))
            if request.include_revision:
                selected.update(entity.primary_key)
            matched = [
                row
                for row in self._rows.get(entity.message_type, {}).get(identity.tenant_id, {}).values()
                if entity.live(row) and entity.visible_to(row, identity) and _matches(row, filter_)
            ]
            order: list[tuple[str, bool]] = []
            for requested in request.sort:
                column = entity.aliases.get(requested.field.strip().lower())
                if column is None:
                    raise _invalid(f"unknown sort column {requested.field}")
                order.append((column, requested.descending))
            order.extend((column, False) for column in entity.primary_key)

            def compare_rows(a: Mapping[str, Any], b: Mapping[str, Any]) -> int:
                for column, descending in order:
                    result = _compare(a.get(column), b.get(column))
                    if result:
                        return -result if descending else result
                return 0

            matched.sort(key=cmp_to_key(compare_rows))
            limit = request.limit if request.limit > 0 else 100
            offset = 0
            if request.page_token:
                try:
                    padded = request.page_token + "=" * (-len(request.page_token) % 4)
                    offset = int(base64.urlsafe_b64decode(padded).decode("ascii"))
                except ValueError:
                    raise _invalid("invalid page_token") from None
                if offset < 0 or offset > len(matched):
                    raise _invalid("invalid page_token offset")
            out = types_pb2.RecordSet()
            if request.include_total:
                out.exact_total = len(matched)
            end = min(offset + limit, len(matched))
            redacted: set[str] = set()
            can_read_masked = _has_scope(identity, "udb:pii:read")
            for row in matched[offset:end]:
                view = {
                    column: entity.wire_value(column, copy.deepcopy(row[column]))
                    for column in selected
                    if column in row
                }
                if not can_read_masked:
                    for column, masked in entity.masked.items():
                        if masked and column in view:
                            view[column] = _MASK
                redacted.update(name for name, value in view.items() if value == _MASK)
                out.records_json.append(json.dumps(view).encode("utf-8"))
                if request.include_revision:
                    out.record_revisions.append(self._revision(identity, entity, entity.key_of(row)))
                wire = out.rows.add()
                for key, value in to_struct(_protobuf_json(view)).fields.items():
                    wire.fields[key].CopyFrom(value)
            out.redacted_fields.extend(sorted(redacted))
            out.has_more = len(out.records_json) >= limit
            if out.has_more and end < len(matched):
                out.next_page_token = base64.urlsafe_b64encode(str(end).encode()).rstrip(b"=").decode("ascii")
            return out

    def _keyed_write(self, call: _Call, operation: str, request: Any, write: Callable[[], types_pb2.MutationResponse]) -> types_pb2.MutationResponse:
        """Only successful writes keep a receipt, and the receipt and row change become
        visible together. Replay precedes CAS and row matching, so a lost-response retry
        does not fail because the first write changed a row."""
        key = request.idempotency_key
        if not key:
            return write()
        if any(char.isspace() or unicodedata.category(char) == "Cc" for char in key):
            raise _violation(
                grpc.StatusCode.INVALID_ARGUMENT, "", "idempotency_key must contain no whitespace or control characters",
                error_pb2.ERROR_KIND_VALIDATION, "idempotency_key", "must contain no whitespace or control characters",
            )
        entity = self._entity(request.message_type)
        identity = call.identity
        digest = hashlib.sha256(_json_text(entity.mutation_inputs(identity, request)).encode("utf-8")).hexdigest()
        scope = (identity.tenant_id, identity.project_id, request.message_type, operation, key)
        receipt = self._receipts.get(scope)
        if receipt is not None:
            if receipt[0] != digest:
                raise _violation(
                    grpc.StatusCode.FAILED_PRECONDITION, "UDB_IDEMPOTENCY_REUSE",
                    "idempotency_key was already used for a different request",
                    error_pb2.ERROR_KIND_CONFLICT, "idempotency_key", "already used with different inputs",
                    "Reuse a key only to retry identical inputs; use a new key for a different write.",
                )
            replay = types_pb2.MutationResponse()
            replay.CopyFrom(receipt[1])
            replay.was_duplicate = True
            return replay
        response = write()
        kept = types_pb2.MutationResponse()
        kept.CopyFrom(response)
        self._receipts[scope] = (digest, kept)
        return response

    def _upsert(self, call: _Call, request: types_pb2.UpsertRequest) -> types_pb2.MutationResponse:
        with self._lock:
            return self._keyed_write(call, "upsert", request, lambda: self._write(call.identity, request, False))

    def _insert(self, call: _Call, request: types_pb2.UpsertRequest) -> types_pb2.MutationResponse:
        with self._lock:
            return self._keyed_write(call, "insert", request, lambda: self._write(call.identity, request, True))

    def _write(self, identity: Identity, request: types_pb2.UpsertRequest, insert_only: bool) -> types_pb2.MutationResponse:
        tenant = identity.tenant_id
        if insert_only and (request.conflict_fields or len(request.expected.fields) > 0):
            raise _invalid("Insert does not accept conflict_fields or expected")
        entity = self._entity(request.message_type)
        try:
            record = _record_of(request)
        except ValueError as error:
            raise _invalid(str(error)) from error
        record = entity.normalize(record)
        entity.bind_record_scope(record, identity)
        entity.conflict_fields(list(request.conflict_fields))
        entity.defaults(record)
        key = entity.key_of(record)
        by_tenant = self._rows.get(entity.message_type, {})
        if insert_only:
            if any(key in rows for rows in by_tenant.values()):
                raise _unique_refusal(f"{entity.table_name}_pkey")
        else:
            for owner, rows in by_tenant.items():
                existing = rows.get(key)
                if existing is not None and (owner != tenant or not entity.same_record_scope(existing, record)):
                    if len(request.expected.fields) > 0:
                        raise _refuse(
                            grpc.StatusCode.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING",
                            "compare-and-swap target does not exist in the verified scope",
                        )
                    # The native ON CONFLICT scope guard leaves a foreign row untouched.
                    return types_pb2.MutationResponse(mutation_id=key, affected_rows=0)
        table = self._table(entity.message_type, tenant)
        expected = entity.expected_columns(_struct_map(request.expected))
        if expected:
            current = table.get(key)
            if current is None:
                raise _refuse(
                    grpc.StatusCode.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING",
                    "compare-and-swap precondition failed: the target row does not exist",
                )
            if not _holds(current, expected):
                raise _refuse(
                    grpc.StatusCode.FAILED_PRECONDITION, "UDB_CAS_CONFLICT",
                    "compare-and-swap precondition failed: a field did not match the current row",
                )
        if entity.soft_delete and not insert_only and entity.soft_delete not in record:
            record[entity.soft_delete] = None
        self._validate_row(entity, tenant, key, record)
        current = table.get(key)
        if current is not None and not insert_only:
            # Isolation columns are excluded from the native conflict UPDATE SET.
            for column in (entity.tenant_column, entity.project_column):
                if column and column in current:
                    record[column] = copy.deepcopy(current[column])
        table[key] = record
        return types_pb2.MutationResponse(
            mutation_id=key, affected_rows=1, revision=self._bump_revision(identity, entity, key)
        )

    def _validate_row(self, entity: _Entity, tenant: str, key: str, row: Mapping[str, Any]) -> None:
        for name, column in entity.options.items():
            if (column.not_null or column.primary_key) and row.get(name) is None:
                raise _column_refusal(name)
        for other_tenant, rows in self._rows.get(entity.message_type, {}).items():
            for other_key, stored in rows.items():
                if other_tenant == tenant and other_key == key:
                    continue
                if other_key == key:
                    raise _unique_refusal(f"{entity.table_name}_pkey")
                for unique in entity.unique:
                    try:
                        included = entity.in_unique(unique, row)
                        other_included = entity.in_unique(unique, stored)
                    except ValueError as error:
                        raise _refuse(grpc.StatusCode.FAILED_PRECONDITION, "", str(error)) from error
                    if not included or not other_included:
                        continue
                    # SQL UNIQUE treats NULLs as distinct.
                    if all(
                        row.get(name) is not None
                        and stored.get(name) is not None
                        and _equal(row.get(name), stored.get(name))
                        for name in unique.columns
                    ):
                        raise _unique_refusal(unique.name)

    def _matching_keys(self, entity: _Entity, identity: Identity, filter_: Mapping[str, Any], table: Mapping[str, Mapping[str, Any]]) -> list[str]:
        return [
            key
            for key, row in table.items()
            if entity.live(row) and entity.visible_to(row, identity) and _matches(row, filter_)
        ]

    def _guard(self, entity: _Entity, identity: Identity, request: Any, filter_: Mapping[str, Any], expected: Mapping[str, Any], keys: Sequence[str], table: Mapping[str, Mapping[str, Any]]) -> None:
        if expected:
            if not keys:
                raise _refuse(grpc.StatusCode.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "CAS row is missing")
            if len(keys) != 1 or not _holds(table[keys[0]], expected):
                raise _refuse(grpc.StatusCode.FAILED_PRECONDITION, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed")
        self._check_revision(identity, entity, filter_, request.expected_revision)
        want = request.require_affected
        if want > 0 and len(keys) != want:
            raise _refuse(
                grpc.StatusCode.NOT_FOUND, "UDB_NO_ROWS_AFFECTED",
                f"the write matched {len(keys)} row(s) but require_affected is {want}; nothing was changed",
            )

    def _update(self, call: _Call, request: types_pb2.UpdateRequest) -> types_pb2.MutationResponse:
        with self._lock:
            return self._keyed_write(call, "update", request, lambda: self._update_locked(call.identity, request))

    def _update_locked(self, identity: Identity, request: types_pb2.UpdateRequest) -> types_pb2.MutationResponse:
        tenant = identity.tenant_id
        entity = self._entity(request.message_type)
        filter_ = entity.query_filter(_struct_map(request.filter))
        if not filter_:
            raise _invalid("an update needs a filter")
        entity.check_tenant(filter_, tenant)
        raw_changes = _struct_map(request.changes)
        entity.update_columns(raw_changes, list(request.increments))
        expected = entity.expected_columns(_struct_map(request.expected))
        if expected:
            entity.validate_cas_key(filter_)
        changes = entity.normalize(raw_changes)
        table = self._table(entity.message_type, tenant)
        keys = self._matching_keys(entity, identity, filter_, table)
        self._guard(entity, identity, request, filter_, expected, keys, table)
        # Build the entire statement result before publishing it. A late unique or NOT
        # NULL violation rolls back every earlier matched row, like PostgreSQL.
        candidate = {key: row for key, row in table.items() if key not in keys}
        changed: list[str] = []
        for key in keys:
            row = copy.deepcopy(table[key])
            for name, value in changes.items():
                row[name] = copy.deepcopy(value)
            for item in request.increments:
                name = entity.aliases.get(item.column.strip().lower())
                if name is None:
                    raise _invalid("unknown increment column")
                row[name] = _increment(row.get(name), item.delta, _integer_column(entity.options[name]))
            new_key = entity.key_of(row)
            if new_key in candidate:
                raise _unique_refusal(f"{entity.table_name}_pkey")
            candidate[new_key] = row
            changed.append(new_key)
        self._rows[entity.message_type][tenant] = candidate
        try:
            for key in changed:
                self._validate_row(entity, tenant, key, candidate[key])
        except _Refusal:
            self._rows[entity.message_type][tenant] = table
            raise
        response = types_pb2.MutationResponse(affected_rows=len(keys))
        if keys:
            try:
                revision_key = entity.revision_filter_key(identity, filter_)
            except _Refusal:
                pass
            else:
                response.revision = self._bump_revision(identity, entity, revision_key)
        return response

    def _delete(self, call: _Call, request: types_pb2.DeleteRequest) -> types_pb2.MutationResponse:
        with self._lock:
            return self._keyed_write(call, "delete", request, lambda: self._delete_locked(call.identity, request))

    def _delete_locked(self, identity: Identity, request: types_pb2.DeleteRequest) -> types_pb2.MutationResponse:
        tenant = identity.tenant_id
        entity = self._entity(request.message_type)
        filter_ = entity.query_filter(_struct_map(request.filter))
        if not filter_:
            raise _invalid("a delete needs a filter")
        entity.check_tenant(filter_, tenant)
        expected = entity.expected_columns(_struct_map(request.expected))
        if expected:
            entity.validate_cas_key(filter_)
        table = self._table(entity.message_type, tenant)
        keys = self._matching_keys(entity, identity, filter_, table)
        self._guard(entity, identity, request, filter_, expected, keys, table)
        for key in keys:
            if not entity.soft_delete:
                del table[key]
            else:
                row = copy.deepcopy(table[key])
                row[entity.soft_delete] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                table[key] = row
        return types_pb2.MutationResponse(affected_rows=len(keys))

    # Revisions: entries outlive a delete, so re-inserting a key never revives an
    # earlier revision.

    def _revision_scope(self, identity: Identity, entity: _Entity, key: str) -> tuple[str, ...]:
        return (identity.tenant_id, identity.project_id, entity.message_type, key)

    def _bump_revision(self, identity: Identity, entity: _Entity, key: str) -> str:
        scope = self._revision_scope(identity, entity, key)
        self._revisions[scope] = self._revisions.get(scope, 0) + 1
        return str(self._revisions[scope])

    def _revision(self, identity: Identity, entity: _Entity, key: str) -> str:
        revision = self._revisions.get(self._revision_scope(identity, entity, key), 0)
        return str(revision) if revision else ""

    def _check_revision(self, identity: Identity, entity: _Entity, filter_: Mapping[str, Any], expected: str) -> None:
        expected = expected.strip()
        if not expected:
            return
        key = entity.revision_filter_key(identity, filter_)
        try:
            revision = int(expected)
        except ValueError:
            raise _violation(
                grpc.StatusCode.INVALID_ARGUMENT, "", "expected_revision is not a valid revision token",
                error_pb2.ERROR_KIND_VALIDATION, "expected_revision",
                "must be an opaque revision token previously returned by the broker",
            ) from None
        if self._revisions.get(self._revision_scope(identity, entity, key)) != revision:
            raise _violation(
                grpc.StatusCode.FAILED_PRECONDITION, "UDB_REVISION_CONFLICT",
                "revision precondition failed: the row's current revision differs from expected_revision, "
                "or the row is not revision-tracked",
                error_pb2.ERROR_KIND_CONFLICT, "expected_revision", "the current revision differs from the expected revision",
            )

    # Transactions

    def _begin_tx(self, request_iterator: Iterator[types_pb2.Mutation], context: grpc.ServicerContext) -> Iterator[types_pb2.TxStatus]:
        call = _Call(context)
        try:
            self._before_rpc(call, "BeginTx", None)
            yield from self._run_tx(call, request_iterator)
        except _Refusal as refusal:
            self._abort(context, refusal)

    def _run_tx(self, call: _Call, request_iterator: Iterator[types_pb2.Mutation]) -> Iterator[types_pb2.TxStatus]:
        mutations: list[types_pb2.Mutation] = []
        terminal: types_pb2.Mutation | None = None
        for mutation in request_iterator:
            if terminal is not None:
                raise _invalid("mutation after transaction terminal marker")
            self._bind_body(call, mutation.context)
            if mutation.commit and mutation.rollback:
                raise _invalid("commit and rollback cannot both be set")
            if mutation.operation:
                operation = mutation.operation.lower()
                mutation.operation = operation
                resource = mutation.message_type
                if operation in ("upsert", "insert"):
                    action = "Upsert"
                elif operation == "update":
                    action = "Update"
                elif operation == "delete":
                    action = "Delete"
                elif operation == "enqueue_outbox_event":
                    action = "EnqueueOutboxEvent"
                    resource = mutation.collection or mutation.message_type
                else:
                    raise _invalid(f"unsupported transaction operation {operation}")
                if mutation.idempotency_key.strip():
                    raise _violation(
                        grpc.StatusCode.INVALID_ARGUMENT, "",
                        "this transaction path cannot honour idempotency_key; nothing was written",
                        error_pb2.ERROR_KIND_VALIDATION, f"mutations[{len(mutations)}].idempotency_key",
                        "not supported on this operation inside BeginTx; leave it empty",
                    )
                self._authorize(call, resource, action)
                mutations.append(mutation)
            if mutation.commit or mutation.rollback:
                terminal = mutation
        if terminal is None:
            raise _refuse(grpc.StatusCode.FAILED_PRECONDITION, "", "transaction closed without commit or rollback")
        if terminal.rollback:
            yield types_pb2.TxStatus(tx_id=terminal.tx_id, state=types_pb2.TxStatus.TX_STATE_ROLLED_BACK)
            return
        with self._lock:
            rows = copy.deepcopy(self._rows)
            revisions = dict(self._revisions)
            events = len(self._events)
            try:
                self._apply_tx(call.identity, mutations)
            except _Refusal:
                self._rows, self._revisions = rows, revisions
                del self._events[events:]
                raise
        yield types_pb2.TxStatus(
            tx_id=terminal.tx_id,
            state=types_pb2.TxStatus.TX_STATE_COMMITTED,
            message=f"{len(mutations)} mutation(s) committed",
        )

    def _apply_tx(self, identity: Identity, mutations: Sequence[types_pb2.Mutation]) -> None:
        t = types_pb2
        for m in mutations:
            if m.operation in ("upsert", "insert"):
                request = t.UpsertRequest(message_type=m.message_type, record_json=m.record_json)
                if m.HasField("payload"):
                    request.payload.CopyFrom(m.payload)
                if m.HasField("expected"):
                    request.expected.CopyFrom(m.expected)
                self._write(identity, request, m.operation == "insert")
            elif m.operation == "update":
                request = t.UpdateRequest(message_type=m.message_type, filter=m.filter, changes=m.changes, expected=m.expected)
                request.increments.extend(m.increments)
                self._update_locked(identity, request)
            elif m.operation == "delete":
                self._delete_locked(identity, t.DeleteRequest(message_type=m.message_type, filter=m.filter, expected=m.expected))
            elif m.operation == "enqueue_outbox_event":
                envelope = _struct_map(m.payload)
                if not m.object_key or envelope.get("document_id") != m.object_key:
                    raise _invalid("outbox partition_key must equal payload.document_id")
                given = envelope.get("tenant_id")
                if isinstance(given, str) and given and given != identity.tenant_id:
                    raise _refuse(grpc.StatusCode.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "outbox tenant differs from verified tenant")
                given = envelope.get("project_id")
                if isinstance(given, str) and given and given != identity.project_id:
                    raise _refuse(grpc.StatusCode.PERMISSION_DENIED, "UDB_POLICY_DENIED", "outbox project differs from verified project")
                if (
                    envelope.get("event_id") is None
                    or envelope.get("event_type") is None
                    or envelope.get("envelope_version") != float(EVENT_ENVELOPE_VERSION)
                ):
                    raise _invalid("outbox needs a versioned event envelope")
                envelope["tenant_id"], envelope["project_id"] = identity.tenant_id, identity.project_id
                self._events.append(Event(m.collection or m.message_type, m.object_key, copy.deepcopy(envelope)))
            else:
                raise _invalid(f"unsupported transaction operation {m.operation}")

    # CDC

    def _publish_cdc(self, request: types_pb2.CDCSubscriptionRequest, context: grpc.ServicerContext) -> Iterator[udb_events_pb2.CDCEnvelope]:
        call = _Call(context)
        try:
            self._before_rpc(call, "PublishCDC", None)
            self._bind_body(call, request.context)
            yield from self._stream_cdc(call, request)
        except _Refusal as refusal:
            self._abort(context, refusal)

    def _stream_cdc(self, call: _Call, request: types_pb2.CDCSubscriptionRequest) -> Iterator[udb_events_pb2.CDCEnvelope]:
        identity = call.identity
        pattern = _topic_pattern(request.topic_pattern)
        self._authorize(call, pattern, "PublishCDC")
        name = owner = ""
        if request.consumer_name.strip():
            name = _consumer_name(request.consumer_name)
            owner = _consumer_owner(identity)
        with self._lock:
            position = 0
            if name:
                key = _cursor_key(identity, name, pattern)
                cursor = self._cursors.get(key)
                if cursor is not None and cursor["owner"] != owner:
                    raise _cursor_owner_error()
                if cursor is None:
                    cursor = {"owner": owner, "index": -1, "id": ""}
                    self._cursors[key] = cursor
                if not request.since_event_id:
                    position = cursor["index"] + 1
            if request.since_event_id:
                for index, event in enumerate(self._events):
                    if event.envelope.get("event_id") == request.since_event_id and _event_in_scope(event, identity, pattern):
                        position = index + 1
                        break
                else:
                    raise _refuse(grpc.StatusCode.NOT_FOUND, "", "CDC resume event is unknown or outside the verified scope")
        while call.context.is_active():
            if time.time() >= call.expires:
                raise _refuse(grpc.StatusCode.UNAUTHENTICATED, "", "bearer expired during CDC stream")
            self._authorize(call, pattern, "PublishCDC")
            with self._lock:
                pending = list(self._events[position:])
                position = len(self._events)
            for event in pending:
                if _event_in_scope(event, identity, pattern):
                    yield udb_events_pb2.CDCEnvelope(
                        event_id=str(event.envelope.get("event_id", "")),
                        topic=event.topic,
                        partition_key=event.partition_key,
                        payload_json=json.dumps(event.envelope),
                    )
            time.sleep(0.02)

    def _ack(self, call: _Call, request: types_pb2.AckCdcEventsRequest) -> types_pb2.AckCdcEventsResponse:
        return self._settle(call, request.consumer_name, request.topic_pattern, request.event_id, None)

    def _nack(self, call: _Call, request: types_pb2.NackCdcEventsRequest) -> types_pb2.AckCdcEventsResponse:
        if (
            request.attempts < 1
            or request.attempts > 1000
            or len(request.failure_reason.encode("utf-8")) > 2048
        ):
            raise _invalid("invalid handler attempts or diagnostic length")
        return self._settle(call, request.consumer_name, request.topic_pattern, request.event_id, request)

    def _settle(self, call: _Call, consumer: str, topic_pattern: str, event_id: str, failure: types_pb2.NackCdcEventsRequest | None) -> types_pb2.AckCdcEventsResponse:
        name = _consumer_name(consumer)
        if not _UUID_TEXT.match(event_id):
            raise _invalid("event_id must be a UUID")
        pattern = _topic_pattern(topic_pattern)
        self._authorize(call, pattern, "PublishCDC")
        identity = call.identity
        owner = _consumer_owner(identity)
        with self._lock:
            index = next(
                (i for i, event in enumerate(self._events)
                 if event.envelope.get("event_id") == event_id and _event_in_scope(event, identity, pattern)),
                -1,
            )
            if index < 0:
                raise _refuse(
                    grpc.StatusCode.PERMISSION_DENIED, "UDB_POLICY_DENIED",
                    "acknowledged CDC event is not retained in the verified scope",
                )
            key = _cursor_key(identity, name, pattern)
            cursor = self._cursors.get(key)
            if cursor is not None and cursor["owner"] != owner:
                raise _cursor_owner_error()
            if cursor is None:
                cursor = {"owner": owner, "index": -1, "id": ""}
            if failure is not None:
                self._dead_letter(identity, self._events[index], event_id, owner, name, pattern, failure)
            if index > cursor["index"]:
                cursor["index"], cursor["id"] = index, event_id
            self._cursors[key] = cursor
        return types_pb2.AckCdcEventsResponse(
            consumer_name=name, topic_pattern=pattern, event_id=event_id, acked_at_unix=int(time.time())
        )

    def _dead_letter(self, identity: Identity, event: Event, event_id: str, owner: str, name: str, pattern: str, failure: types_pb2.NackCdcEventsRequest) -> None:
        now = int(time.time())
        stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        entry = self._dlq.get(event_id)
        if entry is None:
            entry = {
                "identity": identity,
                "event": event,
                "failures": {},
                "metadata": {
                    "service": "udb-cdc-consumer", "error_type": "CONSUMER_HANDLER_FAILED",
                    "error_message": failure.failure_reason, "failed_at": stamp, "retry_count": 0,
                },
                "record": types_pb2.DlqEventRecord(
                    dlq_id=str(uuid.uuid4()), event_id=event_id, topic=event.topic,
                    error_type="CONSUMER_HANDLER_FAILED", error_message=failure.failure_reason, created_at_unix=now,
                ),
            }
        digest = hashlib.sha256()
        for part in (owner, name, pattern):
            digest.update(len(part.encode("utf-8")).to_bytes(8, "big"))
            digest.update(part.encode("utf-8"))
        failure_key = digest.hexdigest()
        if failure_key not in entry["failures"]:
            entry["failures"][failure_key] = {
                "owner_identity": owner, "consumer_name": name, "topic_pattern": pattern,
                "attempts": failure.attempts, "failure_reason": failure.failure_reason, "failed_at": stamp,
            }
            entry["record"].payload_json = json.dumps(
                {"failed_event": event.envelope, "failure_metadata": entry["metadata"], "consumer_failures": entry["failures"]}
            ).encode("utf-8")
            entry["record"].status = "QUARANTINED"
            entry["record"].updated_at_unix = now
            self._dlq[event_id] = entry

    # Operator DLQ APIs

    def _dlq_entry(self, call: _Call, dlq_id: str) -> dict[str, Any]:
        for entry in self._dlq.values():
            if entry["record"].dlq_id == dlq_id and _same_domain(entry["identity"], call.identity):
                return entry
        raise _refuse(grpc.StatusCode.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", "DLQ event not found in verified tenant/project")

    def _list_dlq(self, call: _Call, request: types_pb2.DlqListRequest) -> types_pb2.DlqListResponse:
        with self._lock:
            events = sorted(
                (
                    entry["record"]
                    for entry in self._dlq.values()
                    if _same_domain(entry["identity"], call.identity)
                    and (not request.topic or request.topic == entry["record"].topic)
                    and (not request.status_filter or request.status_filter == entry["record"].status)
                ),
                key=lambda record: record.dlq_id,
            )
            offset = 0
            if request.page_token:
                try:
                    offset = int(request.page_token)
                except ValueError:
                    raise _invalid("invalid DLQ page token") from None
                if offset < 0 or offset > len(events):
                    raise _invalid("invalid DLQ page token")
            limit = request.limit if request.limit > 0 else 100
            end = min(len(events), offset + limit)
            out = types_pb2.DlqListResponse(total_count=len(events))
            out.events.extend(events[offset:end])
            if end < len(events):
                out.next_page_token = str(end)
            return out

    def _get_dlq(self, call: _Call, request: types_pb2.DlqEventRequest) -> types_pb2.DlqEventResponse:
        with self._lock:
            return types_pb2.DlqEventResponse(event=self._dlq_entry(call, request.dlq_id)["record"])

    def _replay_dlq(self, call: _Call, request: types_pb2.DlqActionRequest) -> types_pb2.MutationResponse:
        with self._lock:
            entry = self._dlq_entry(call, request.dlq_id)
            if entry["record"].status not in ("OPEN", "RETRYING", "QUARANTINED"):
                raise _refuse(grpc.StatusCode.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", "DLQ event is not replayable")
            source: Event = entry["event"]
            envelope = copy.deepcopy(source.envelope)
            # The canonical outbox's event id is unique, so preserving it never adds a
            # duplicate retained event.
            if not request.preserve_event_id:
                envelope["event_id"] = str(uuid.uuid4())
                self._events.append(Event(source.topic, source.partition_key, envelope))
            entry["record"].status = "REPLAYED"
            entry["record"].updated_at_unix = int(time.time())
            return types_pb2.MutationResponse(mutation_id=str(envelope["event_id"]), affected_rows=1)

    def _set_dlq_status(self, call: _Call, request: types_pb2.DlqActionRequest, status: str) -> types_pb2.MutationResponse:
        with self._lock:
            entry = self._dlq_entry(call, request.dlq_id)
            entry["record"].status = status
            entry["record"].updated_at_unix = int(time.time())
            return types_pb2.MutationResponse(mutation_id=request.dlq_id, affected_rows=1)

    def _dismiss_dlq(self, call: _Call, request: types_pb2.DlqActionRequest) -> types_pb2.MutationResponse:
        return self._set_dlq_status(call, request, "DISMISSED")

    def _quarantine_dlq(self, call: _Call, request: types_pb2.DlqActionRequest) -> types_pb2.MutationResponse:
        return self._set_dlq_status(call, request, "QUARANTINED")


def _same_domain(a: Identity, b: Identity) -> bool:
    return a.tenant_id == b.tenant_id and a.project_id == b.project_id


def _cursor_owner_error() -> _Refusal:
    return _refuse(
        grpc.StatusCode.PERMISSION_DENIED, "UDB_POLICY_DENIED",
        "named CDC consumer belongs to another verified principal",
    )


def new(*entities: Any) -> Fake:
    """Start a fake serving the given entity messages, with an allow-all test policy.
    Use it as a context manager so it stops with the test::

        with udbtest.new(File) as fake:
            project = fake.client(tenant)
    """
    fake = Fake()
    for message in entities:
        fake.register(message)
    return fake.serve()


def _policy_text(policy_id: Any, name: str, value: Any) -> str:
    """A scalar as text, as the YAML decoder reads it into a string field."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return str(value)
    raise ValueError(f"udbtest: policy schema: policy {policy_id!r} field {name} must be a scalar")


def _policy_of(raw: Any, document_tenant: str) -> Policy:
    if not isinstance(raw, Mapping):
        raise ValueError("udbtest: policy schema: each policy must be a mapping")
    policy_id = raw.get("id")
    text = {
        name: _policy_text(policy_id, name, raw.get(name))
        for name in ("id", "effect", "tenant", "project", "subject", "role", "action", "resource", "purpose", "relationship")
    }
    if text["effect"] not in ("", "allow", "deny"):
        raise ValueError(f"udbtest: policy {text['id']!r} has invalid effect")
    if not text["tenant"].strip():
        text["tenant"] = document_tenant
    priority = raw.get("priority") or 0
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise ValueError(f"udbtest: policy schema: policy {text['id']!r} priority must be an integer")
    enabled = raw.get("enabled")
    if enabled is not None and not isinstance(enabled, bool):
        raise ValueError(f"udbtest: policy schema: policy {text['id']!r} enabled must be a boolean")
    conditions = raw.get("conditions") or {}
    if not isinstance(conditions, Mapping):
        raise ValueError(f"udbtest: policy schema: policy {text['id']!r} conditions must be a mapping")
    scopes = raw.get("required_scopes") or []
    if not isinstance(scopes, list):
        raise ValueError(f"udbtest: policy schema: policy {text['id']!r} required_scopes must be a list")
    return Policy(
        priority=priority,
        enabled=enabled,
        conditions={str(key): _policy_text(policy_id, "conditions", value) for key, value in conditions.items()},
        required_scopes=tuple(_policy_text(policy_id, "required_scopes", scope) for scope in scopes),
        **text,
    )


def load_policies(path: str) -> list[Policy]:
    """Read the JSON/YAML policy file ``udb policy diff|apply`` reads: a list of rules,
    or a document with ``policies`` and an optional default ``tenant``. Reading it needs
    PyYAML, from the ``test`` extra. A malformed file raises ``ValueError``."""
    try:
        import yaml
    except ImportError:
        raise RuntimeError(
            "udbtest: reading a policy file needs PyYAML; install it with: pip install 'udb-client[test]'"
        ) from None
    try:
        with open(path, encoding="utf-8") as file:
            documents = list(yaml.safe_load_all(file))
    except OSError as error:
        raise ValueError(f"udbtest: policy file: {error}") from error
    except (yaml.YAMLError, UnicodeDecodeError) as error:
        raise ValueError(f"udbtest: parse policy file: {error}") from error
    if len(documents) > 1:
        raise ValueError("udbtest: policy file must contain one document")
    if not documents or documents[0] is None:
        raise ValueError("udbtest: empty policy document")
    document = documents[0]
    tenant = ""
    if isinstance(document, Mapping):
        tenant = _policy_text("", "tenant", document.get("tenant"))
        rules = document.get("policies")
        if rules is None:
            raise ValueError("udbtest: policy document requires policies")
        if not isinstance(rules, list):
            raise ValueError("udbtest: policy schema: policies must be a list")
    elif isinstance(document, list):
        rules = document
    else:
        raise ValueError("udbtest: expected a policy list or policies document")
    return [_policy_of(rule, tenant) for rule in rules]


def _file_pool(descriptor_set: descriptor_pb2.FileDescriptorSet) -> descriptor_pool.DescriptorPool:
    """A private pool of the set's files. Adding a file needs its dependencies first,
    and the set may list them in any order."""
    pending = {file.name: file for file in descriptor_set.file}
    pool = descriptor_pool.DescriptorPool()
    added: set[str] = set()
    visiting: set[str] = set()

    def add(name: str, importer: str) -> None:
        if name in added:
            return
        file = pending.get(name)
        if file is None:
            raise ValueError(f"udbtest: descriptor set: {importer} imports {name}, which the set lacks")
        if name in visiting:
            raise ValueError(f"udbtest: descriptor set: import cycle through {name}")
        visiting.add(name)
        for dependency in file.dependency:
            add(dependency, name)
        try:
            pool.Add(file)
        except (TypeError, ValueError) as error:
            raise ValueError(f"udbtest: descriptor set: {name}: {error}") from error
        added.add(name)

    for name in pending:
        add(name, name)
    return pool


def new_fake(descriptor_set: descriptor_pb2.FileDescriptorSet, policy_file: str = "") -> Fake:
    """Serve the tables a descriptor set declares, judged by the same JSON/YAML policy
    file as ``udb policy apply``. With no policy file it denies data requests by default.
    :func:`new` remains the convenience fixture with an explicit allow-all test policy;
    use this for policy conformance. Close the result, or use it as a context manager.

    The set must include its imports, as ``--include-imports`` emits them. A set that
    declares no tables raises ``ValueError``, as does a malformed policy file.
    """
    if descriptor_set is None:
        raise ValueError("udbtest: descriptor_set is required")
    pool = _file_pool(descriptor_set)
    policies = load_policies(policy_file) if policy_file else []
    fake = Fake(default_allow=False)
    fake.set_policies(policies)

    def register(descriptor: Any) -> None:
        table = table_options(descriptor)
        if table is not None and table.is_table:
            fake.register(message_factory.GetMessageClass(descriptor))
        for nested in descriptor.nested_types:
            register(nested)

    for file in descriptor_set.file:
        for descriptor in pool.FindFileByName(file.name).message_types_by_name.values():
            register(descriptor)
    if not fake._entities:
        raise ValueError("udbtest: descriptor set declares no tables")
    return fake.serve()
