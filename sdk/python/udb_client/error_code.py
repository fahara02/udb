"""Error classification: branch on what the broker said, never on message text.

The broker's stable ``UDB_*`` reason decides the :class:`ErrorCode` when present,
then the typed kind, then the gRPC code. These helpers follow an exception's
``__cause__`` chain, so they also read a :class:`UdbConflict` or
:class:`UdbNotFound` raised from the broker's refusal.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Iterator

import grpc

from udb.entity.v1 import error_pb2

from .exceptions import UdbConflict, UdbNotFound, UdbRpcError
from .generated_client import UdbDetailedRpcError, _synthesize_transport_error_detail

_ERROR_DETAIL_TRAILER = "udb-error-detail-bin"
_CAS_REASONS = frozenset({"UDB_CAS_CONFLICT", "UDB_CAS_ROW_MISSING", "UDB_REVISION_CONFLICT"})


class ErrorCode(str, enum.Enum):
    """The closed set of ways a udb call fails."""

    UNKNOWN = "unknown"
    #: A conditional write lost: read again and retry.
    CONFLICT = "conflict"
    #: The addressed row or resource does not exist.
    NOT_FOUND = "not_found"
    #: A unique constraint was violated (``ErrorInfo.constraint`` names it).
    UNIQUE = "unique"
    #: A NOT NULL column got no value (``ErrorInfo.column`` names it).
    NOT_NULL = "not_null"
    #: A foreign-key constraint was violated.
    FOREIGN_KEY = "foreign_key"
    #: The request is not valid in the current state.
    PRECONDITION = "precondition"
    #: The caller lacks a scope (``ErrorInfo.missing["scope"]``).
    SCOPE_MISSING = "scope_missing"
    #: No policy rule allows the call (``ErrorInfo.missing["rule"]``).
    POLICY_DENIED = "policy_denied"
    #: A write tried to store a redacted placeholder.
    REDACTED = "redacted"
    #: A rate limit was hit (``ErrorInfo.retry_after`` says when to retry).
    RATE_LIMITED = "rate_limited"
    #: Transient: retry with backoff.
    UNAVAILABLE = "unavailable"
    #: The request itself is malformed.
    INVALID = "invalid"
    #: The credential is missing, expired or invalid.
    UNAUTHENTICATED = "unauthenticated"
    #: A broker defect: report it.
    INTERNAL = "internal"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ErrorInfo:
    """Everything the broker said about a failure."""

    code: ErrorCode = ErrorCode.UNKNOWN
    #: The stable ``UDB_*`` reason; empty when the broker sent none.
    reason: str = ""
    constraint: str = ""
    column: str = ""
    #: One sentence on how to fix the request.
    fix_hint: str = ""
    #: What the caller lacks (scope, rule, purpose, ...).
    missing: dict[str, str] = field(default_factory=dict)
    #: Seconds the broker suggests waiting before a retry.
    retry_after: float = 0.0
    #: The underlying gRPC code; ``None`` when the failure was not a gRPC one.
    grpc_code: grpc.StatusCode | None = None


_REASON_CODES = {
    "UDB_CAS_CONFLICT": ErrorCode.CONFLICT,
    "UDB_CAS_ROW_MISSING": ErrorCode.CONFLICT,
    "UDB_REVISION_CONFLICT": ErrorCode.CONFLICT,
    "UDB_GRANT_OWNED_BY_OTHER": ErrorCode.CONFLICT,
    "UDB_CAS_KEY_NOT_PK": ErrorCode.PRECONDITION,
    "UDB_IDEMPOTENCY_REUSE": ErrorCode.PRECONDITION,
    "UDB_NO_ROWS_AFFECTED": ErrorCode.NOT_FOUND,
    "UDB_UNIQUE_VIOLATION": ErrorCode.UNIQUE,
    "UDB_NOT_NULL_VIOLATION": ErrorCode.NOT_NULL,
    "UDB_UPSERT_INCOMPLETE_ROW": ErrorCode.NOT_NULL,
    "UDB_FOREIGN_KEY_VIOLATION": ErrorCode.FOREIGN_KEY,
    "UDB_TYPE_MISMATCH": ErrorCode.INVALID,
    "UDB_DECODE_FAILED": ErrorCode.INTERNAL,
    "UDB_UNSUPPORTED_FILTER_OPERATOR": ErrorCode.INVALID,
    "UDB_NULL_COMPARISON": ErrorCode.INVALID,
    "UDB_TENANT_MISMATCH": ErrorCode.POLICY_DENIED,
    "UDB_TABLE_NOT_TENANT_SCOPED": ErrorCode.INVALID,
    "UDB_SCOPE_MISSING": ErrorCode.SCOPE_MISSING,
    "UDB_POLICY_DENIED": ErrorCode.POLICY_DENIED,
    "UDB_REDACTED_VALUE_WRITE": ErrorCode.REDACTED,
    "UDB_RATE_LIMITED": ErrorCode.RATE_LIMITED,
    "UDB_UNKNOWN_MESSAGE_TYPE": ErrorCode.INVALID,
    "UDB_ENVELOPE_VERSION_UNSUPPORTED": ErrorCode.PRECONDITION,
}

_KIND_CODES = {
    error_pb2.ERROR_KIND_CONFLICT: ErrorCode.CONFLICT,
    error_pb2.ERROR_KIND_NOT_FOUND: ErrorCode.NOT_FOUND,
    error_pb2.ERROR_KIND_UNIQUE: ErrorCode.UNIQUE,
    error_pb2.ERROR_KIND_NOT_NULL: ErrorCode.NOT_NULL,
    error_pb2.ERROR_KIND_FOREIGN_KEY: ErrorCode.FOREIGN_KEY,
    error_pb2.ERROR_KIND_PERMISSION: ErrorCode.POLICY_DENIED,
    error_pb2.ERROR_KIND_REDACTED: ErrorCode.REDACTED,
    error_pb2.ERROR_KIND_RATE_LIMITED: ErrorCode.RATE_LIMITED,
    error_pb2.ERROR_KIND_QUOTA: ErrorCode.RATE_LIMITED,
    error_pb2.ERROR_KIND_RETRYABLE: ErrorCode.UNAVAILABLE,
    error_pb2.ERROR_KIND_VALIDATION: ErrorCode.INVALID,
    error_pb2.ERROR_KIND_INTERNAL: ErrorCode.INTERNAL,
}

_GRPC_CODES = {
    grpc.StatusCode.NOT_FOUND: ErrorCode.NOT_FOUND,
    grpc.StatusCode.ALREADY_EXISTS: ErrorCode.UNIQUE,
    grpc.StatusCode.FAILED_PRECONDITION: ErrorCode.PRECONDITION,
    grpc.StatusCode.PERMISSION_DENIED: ErrorCode.POLICY_DENIED,
    grpc.StatusCode.RESOURCE_EXHAUSTED: ErrorCode.RATE_LIMITED,
    grpc.StatusCode.UNAVAILABLE: ErrorCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED: ErrorCode.UNAVAILABLE,
    grpc.StatusCode.INVALID_ARGUMENT: ErrorCode.INVALID,
    grpc.StatusCode.UNAUTHENTICATED: ErrorCode.UNAUTHENTICATED,
    grpc.StatusCode.INTERNAL: ErrorCode.INTERNAL,
}


def _trailer_detail(error: Any) -> bytes | None:
    """The detail trailer of a sync or asyncio gRPC error. The asyncio error's
    trailers are a ``grpc.aio.Metadata``, not a sequence, so they are iterated."""
    getter = getattr(error, "trailing_metadata", None)
    if not callable(getter):
        return None
    try:
        trailers = getter()
    except Exception:  # A failed call may have no trailers to read.
        return None
    found: bytes | None = None
    for entry in trailers or ():
        if isinstance(entry, tuple) and len(entry) == 2 and entry[0] == _ERROR_DETAIL_TRAILER:
            if isinstance(entry[1], (bytes, bytearray)):
                found = bytes(entry[1])
    return found


def map_rpc_error(rpc_name: str, error: grpc.RpcError) -> UdbRpcError:
    """The typed error for a failed call: a :class:`UdbDetailedRpcError` when the
    broker attached an ErrorDetail or the failure was at the transport."""
    detail = _trailer_detail(error) or _synthesize_transport_error_detail(error)
    if detail is not None:
        return UdbDetailedRpcError(rpc_name, error, detail)
    return UdbRpcError(rpc_name, error)


def _chain(error: BaseException | None) -> Iterator[BaseException]:
    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        yield error
        error = error.__cause__


def _rpc_error(error: BaseException | None) -> UdbRpcError | None:
    for item in _chain(error):
        if isinstance(item, UdbRpcError):
            return item
    return None


def inspect(error: BaseException | None) -> ErrorInfo:
    """Read everything the broker said about ``error``. ``None`` is the empty
    :class:`ErrorInfo`; an error that is not a udb call failure is ``UNKNOWN``."""
    if error is None:
        return ErrorInfo()
    rpc = _rpc_error(error)
    grpc_code = rpc.code if rpc is not None else None
    detail = getattr(rpc, "error_detail", None) if rpc is not None else None
    fields: dict[str, Any] = {"grpc_code": grpc_code}
    if detail is not None:
        fields.update(
            reason=detail.reason,
            constraint=detail.constraint,
            column=detail.column,
            fix_hint=detail.fix_hint,
            missing=dict(detail.missing),
            retry_after=detail.retry_after_ms / 1000.0,
        )
        code = _REASON_CODES.get(detail.reason)
        if code is not None:
            return ErrorInfo(code=code, **fields)
        if detail.kind != error_pb2.ERROR_KIND_VALIDATION:
            code = _KIND_CODES.get(detail.kind)
            if code is not None:
                return ErrorInfo(code=code, **fields)
    chain = list(_chain(error))
    if any(isinstance(item, UdbConflict) for item in chain):
        return ErrorInfo(code=ErrorCode.CONFLICT, **fields)
    if any(isinstance(item, UdbNotFound) for item in chain):
        return ErrorInfo(code=ErrorCode.NOT_FOUND, **fields)
    return ErrorInfo(code=_GRPC_CODES.get(grpc_code, ErrorCode.UNKNOWN), **fields)


def code_of(error: BaseException | None) -> ErrorCode:
    """``inspect(error).code``."""
    return inspect(error).code


def is_unique(error: BaseException | None, constraint: str | None = None) -> bool:
    """A unique-constraint violation; with ``constraint``, only that constraint."""
    info = inspect(error)
    if info.code is not ErrorCode.UNIQUE:
        return False
    return constraint is None or info.constraint == constraint


def is_not_found(error: BaseException | None) -> bool:
    """A missing row or resource."""
    return code_of(error) is ErrorCode.NOT_FOUND


def is_conflict(error: BaseException | None) -> bool:
    """A lost conditional write."""
    return code_of(error) is ErrorCode.CONFLICT


def missing_scope(error: BaseException | None) -> str:
    """The scope the broker said the caller lacks; ``""`` for any other failure."""
    info = inspect(error)
    if info.code is not ErrorCode.SCOPE_MISSING:
        return ""
    return info.missing.get("scope", "")


def error_reason(error: BaseException | None) -> str:
    """The stable machine-readable reason: the ``UDB_*`` reason when the broker sent
    one, else the policy decision id, else the capability token, else ``""``."""
    rpc = _rpc_error(error)
    detail = getattr(rpc, "error_detail", None) if rpc is not None else None
    if detail is None:
        return ""
    return detail.reason or detail.policy_decision_id or detail.capability_required


def is_cas_conflict(error: BaseException | None) -> bool:
    """A compare-and-swap precondition failure: the row was absent or a field no
    longer matched. Detected only by the typed reason, never the message. Other
    FAILED_PRECONDITION refusals are usage errors a retry cannot fix, and are not
    conflicts."""
    rpc = _rpc_error(error)
    if rpc is None or rpc.code != grpc.StatusCode.FAILED_PRECONDITION:
        return False
    return error_reason(rpc) in _CAS_REASONS


def classify(error: UdbRpcError) -> Exception:
    """The exception a typed store raises for a refusal: :class:`UdbConflict` for a
    lost conditional write, :class:`UdbNotFound` for a missing row, else ``error``.
    Raise it ``from error`` so :func:`inspect` still reads the detail."""
    reason = error_reason(error)
    if reason in _CAS_REASONS:
        return UdbConflict(f"udb: conflict: the row changed since it was read: {error}")
    if reason == "UDB_NO_ROWS_AFFECTED" or error.code == grpc.StatusCode.NOT_FOUND:
        return UdbNotFound(f"udb: not found: {error}")
    return error
