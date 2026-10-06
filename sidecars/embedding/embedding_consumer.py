#!/usr/bin/env python3
"""Ready-made `udb.embedding.work.v1` consumer for the UDB embedding sidecar.

Selected with `UDB_EMBEDDING_SIDECAR_MODE=consumer`. It closes the loop the
broker deliberately leaves open (the broker never runs inference):

    Kafka `udb.embedding.work.v1`  ->  provider batch call  ->  ReportEmbeddingBatch
                                                           \\->  ReportEmbeddingFailure

Delivery semantics are AT-LEAST-ONCE. Offsets are committed manually, and only
after every record of a poll window has been reported to the broker (success
or failure). Only a TRANSIENT report failure (broker unavailable, timeout,
overload) makes the consumer seek back and retry the window, so a crash or
broker outage re-delivers work rather than losing it. A report that cannot
succeed as configured (no bearer token for the tenant, or a non-transient
refusal such as PERMISSION_DENIED) is handled like a malformed record: logged
with the tenant, counted, skipped, and committed past, so one tenant never
stalls a partition. Duplicate delivery is safe: the broker
keys a report by `work_item_id` + `chunk_hash` and the vector upsert is keyed by
point id. Independently, the broker's own visibility-timeout sweep re-emits any
work item that is never acknowledged, and dead-letters it after its attempt
budget.

Everything that talks to Kafka or gRPC sits behind a tiny interface
(`poll/commit/seek/close` and `report_batch/report_failure`) so the batching,
retry and commit-ordering logic is unit-tested with fakes.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol

import embedding_sidecar as sidecar

LOG = logging.getLogger("udb.embedding.consumer")

DEFAULT_TOPIC = "udb.embedding.work.v1"
DEFAULT_GROUP = "udb-embedding-sidecar"
DEFAULT_BROKER_TARGET = "127.0.0.1:50061"
REPORT_BATCH_LIMIT = 256  # the broker's MAX_EMBEDDING_REPORT_BATCH
DOMAIN_KEYS = ("row_pk", "text", "model_id")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = _env(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise sidecar.SidecarError(f"{name} must be an integer, got {raw!r}", 500) from exc
    if value < minimum:
        raise sidecar.SidecarError(f"{name} must be >= {minimum}", 500)
    return value


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = _env(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise sidecar.SidecarError(f"{name} must be a number, got {raw!r}", 500) from exc
    if value < minimum:
        raise sidecar.SidecarError(f"{name} must be >= {minimum}", 500)
    return value


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with full jitter, bounded by `max_attempts`."""

    max_attempts: int = 5
    base_seconds: float = 0.5
    cap_seconds: float = 30.0

    def delay(self, attempt: int, rng: Callable[[], float] = random.random) -> float:
        """Delay before retry number `attempt` (1-based): U(0, min(cap, base*2^(n-1)))."""
        ceiling = min(self.cap_seconds, self.base_seconds * (2 ** max(0, attempt - 1)))
        return ceiling * rng()

    @classmethod
    def from_env(cls, prefix: str, max_attempts: int) -> "RetryPolicy":
        return cls(
            max_attempts=_env_int(f"{prefix}_MAX_ATTEMPTS", max_attempts),
            base_seconds=_env_float(f"{prefix}_BACKOFF_BASE_SECONDS", 0.5),
            cap_seconds=_env_float(f"{prefix}_BACKOFF_CAP_SECONDS", 30.0),
        )


@dataclass(frozen=True)
class ConsumerConfig:
    bootstrap_servers: str = "127.0.0.1:9092"
    topic: str = DEFAULT_TOPIC
    group_id: str = DEFAULT_GROUP
    client_id: str = "udb-embedding-sidecar"
    auto_offset_reset: str = "earliest"
    max_batch_size: int = 64
    max_wait_seconds: float = 0.5
    poll_timeout_seconds: float = 1.0
    max_poll_interval_ms: int = 600_000
    session_timeout_ms: int = 45_000
    kafka_security: dict[str, Any] = field(default_factory=dict)
    provider_retry: RetryPolicy = RetryPolicy()
    report_retry: RetryPolicy = RetryPolicy(max_attempts=5)
    redelivery_backoff: RetryPolicy = RetryPolicy(max_attempts=1_000_000, base_seconds=1.0, cap_seconds=60.0)

    @classmethod
    def from_env(cls) -> "ConsumerConfig":
        security: dict[str, Any] = {}
        protocol = _env("UDB_EMBEDDING_KAFKA_SECURITY_PROTOCOL")
        if protocol:
            security["security_protocol"] = protocol
        for env_name, key in (
            ("UDB_EMBEDDING_KAFKA_SSL_CAFILE", "ssl_cafile"),
            ("UDB_EMBEDDING_KAFKA_SSL_CERTFILE", "ssl_certfile"),
            ("UDB_EMBEDDING_KAFKA_SSL_KEYFILE", "ssl_keyfile"),
            ("UDB_EMBEDDING_KAFKA_SASL_MECHANISM", "sasl_mechanism"),
            ("UDB_EMBEDDING_KAFKA_SASL_USERNAME", "sasl_plain_username"),
            ("UDB_EMBEDDING_KAFKA_SASL_PASSWORD", "sasl_plain_password"),
        ):
            value = _env(env_name)
            if value:
                security[key] = value
        return cls(
            bootstrap_servers=_env(
                "UDB_EMBEDDING_KAFKA_BOOTSTRAP", _env("UDB_KAFKA_BROKERS", "127.0.0.1:9092")
            ),
            topic=_env("UDB_EMBEDDING_KAFKA_TOPIC", DEFAULT_TOPIC),
            group_id=_env("UDB_EMBEDDING_KAFKA_GROUP", DEFAULT_GROUP),
            client_id=_env("UDB_EMBEDDING_KAFKA_CLIENT_ID", "udb-embedding-sidecar"),
            auto_offset_reset=_env("UDB_EMBEDDING_KAFKA_AUTO_OFFSET_RESET", "earliest"),
            max_batch_size=min(
                REPORT_BATCH_LIMIT, _env_int("UDB_EMBEDDING_BATCH_MAX_SIZE", 64)
            ),
            max_wait_seconds=_env_float("UDB_EMBEDDING_BATCH_MAX_WAIT_MS", 500.0) / 1000.0,
            max_poll_interval_ms=_env_int("UDB_EMBEDDING_KAFKA_MAX_POLL_INTERVAL_MS", 600_000),
            session_timeout_ms=_env_int("UDB_EMBEDDING_KAFKA_SESSION_TIMEOUT_MS", 45_000),
            kafka_security=security,
            provider_retry=RetryPolicy.from_env("UDB_EMBEDDING_PROVIDER_RETRY", 5),
            report_retry=RetryPolicy.from_env("UDB_EMBEDDING_REPORT_RETRY", 5),
        )


# ---------------------------------------------------------------------------
# Work decoding
# ---------------------------------------------------------------------------


def unwrap_work_payload(value: Any) -> Any:
    """Return the canonical work object from a published Kafka value.

    The outbox publishes a compliance envelope whose domain fields live under
    `payload` (sometimes nested twice, and sometimes JSON-encoded as a string),
    so descend until the work's identifying keys are present.
    """
    candidate = value
    for _ in range(4):
        if isinstance(candidate, dict) and all(candidate.get(key) for key in DOMAIN_KEYS):
            return candidate
        inner = candidate.get("payload") if isinstance(candidate, dict) else None
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except json.JSONDecodeError:
                return candidate
        if not isinstance(inner, dict):
            return candidate
        candidate = inner
    return candidate


def decode_record_value(raw: bytes | str | None) -> sidecar.WorkItem:
    if raw is None:
        raise sidecar.SidecarError("empty Kafka record (tombstone)")
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        value = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise sidecar.SidecarError(f"work record is not JSON: {exc}") from exc
    return sidecar.work_from_value(unwrap_work_payload(value))


def group_for_provider(
    works: Iterable[sidecar.WorkItem], max_batch_size: int
) -> list[list[sidecar.WorkItem]]:
    """Group by `provider_batch_key` (tenant, project, provider, model,
    dimensions, endpoint ref, task, dtype), preserving arrival order, then split
    each group into chunks of at most `max_batch_size`."""
    groups: dict[tuple[str, ...], list[sidecar.WorkItem]] = {}
    for work in works:
        groups.setdefault(sidecar.provider_batch_key(work), []).append(work)
    batches: list[list[sidecar.WorkItem]] = []
    for items in groups.values():
        for start in range(0, len(items), max_batch_size):
            batches.append(items[start : start + max_batch_size])
    return batches


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------


def is_retryable(error: BaseException) -> bool:
    if isinstance(error, sidecar.SidecarError):
        return bool(error.retryable)
    return isinstance(error, (TimeoutError, ConnectionError))


def call_with_retry(
    operation: Callable[[], Any],
    policy: RetryPolicy,
    *,
    retryable: Callable[[BaseException], bool] = is_retryable,
    sleep: Callable[[float], None] = time.sleep,
    rng: Callable[[], float] = random.random,
    stop: threading.Event | None = None,
) -> Any:
    """Run `operation`, retrying retryable errors with jittered exponential
    backoff up to `policy.max_attempts` total attempts. The last error is
    re-raised; a non-retryable error is raised immediately."""
    attempt = 1
    while True:
        try:
            return operation()
        except Exception as error:  # noqa: BLE001 - classified below
            if not retryable(error) or attempt >= policy.max_attempts:
                raise
            if stop is not None and stop.is_set():
                raise
            delay = policy.delay(attempt, rng)
            LOG.warning("retryable failure (attempt %d/%d), backing off %.2fs: %s",
                        attempt, policy.max_attempts, delay, error)
            sleep(delay)
            attempt += 1


# ---------------------------------------------------------------------------
# Broker reporting
# ---------------------------------------------------------------------------


class ReportTransportError(Exception):
    """A TRANSIENT delivery failure (broker unavailable / timeout / overloaded
    after the report retry budget). Offsets must NOT be committed; the window
    is re-delivered after a backoff. Nothing else rewinds a window."""


class ReportUndeliverable(Exception):
    """A report that can never be delivered by this sidecar as configured:
    no bearer token for the tenant, or a non-transient broker refusal
    (UNAUTHENTICATED, PERMISSION_DENIED, INTERNAL, ...). Handled like a
    malformed record: logged, counted, skipped, and committed past so one
    tenant cannot stall a partition. The broker's visibility-timeout sweep
    re-sends or dead-letters the work item."""

    def __init__(self, tenant_id: str, reason: str) -> None:
        super().__init__(f"tenant {tenant_id}: {reason}")
        self.tenant_id = tenant_id
        self.reason = reason


class MissingBearerToken(ReportUndeliverable):
    def __init__(self, tenant_id: str) -> None:
        super().__init__(tenant_id, "no bearer token configured for this tenant")


# Process-wide counters (the sidecar has no metrics exporter; they are logged
# with every window and exposed for tests/embedding hosts).
COUNTERS: dict[str, int] = {"unreportable_items": 0, "skipped_records": 0}
_COUNTERS_LOCK = threading.Lock()


def _count(name: str, amount: int = 1) -> None:
    with _COUNTERS_LOCK:
        COUNTERS[name] = COUNTERS.get(name, 0) + amount


class Reporter(Protocol):
    def report_batch(
        self, tenant_id: str, project_id: str, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Return one `{work_item_id,row_pk,upserted,error}` per item.
        Raise ReportTransportError for a transient failure (rewind) and
        ReportUndeliverable for one that will not heal (skip + commit)."""

    def report_failure(self, tenant_id: str, project_id: str, failure: dict[str, Any]) -> None:
        """Same error contract as report_batch."""


class ReportRejected(Exception):
    """The broker answered with a permanent, request-level rejection."""


@dataclass
class _TokenSource:
    """Service bearer per tenant.

    A broker bearer's tenant claim must equal the reported tenant (the tenant can
    never be selected by header), so a multi-tenant deployment needs one token
    per tenant. `UDB_EMBEDDING_BEARER_TOKENS_FILE` is a JSON object
    `{"<tenant uuid>": "<jwt>", "*": "<fallback jwt>"}`, re-read whenever its
    mtime changes so rotation needs no restart. `UDB_EMBEDDING_BEARER_TOKEN`
    (or `UDB_EMBEDDING_BEARER_TOKEN_FILE`) is the single-tenant shorthand.
    """

    tokens_file: str = ""
    token_file: str = ""
    token: str = ""
    _cache: dict[str, str] = field(default_factory=dict)
    _mtime: float = -1.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def configured(self) -> bool:
        """False when no bearer source is set at all (auth-disabled dev
        brokers); then calls go out without `authorization`."""
        return bool(self.tokens_file or self.token_file or self.token)

    def for_tenant(self, tenant_id: str) -> str:
        if self.tokens_file:
            path = Path(self.tokens_file)
            with self._lock:
                mtime = path.stat().st_mtime
                if mtime != self._mtime:
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                    if not isinstance(loaded, dict):
                        raise sidecar.SidecarError("bearer tokens file must be a JSON object", 500)
                    self._cache = {str(k): str(v).strip() for k, v in loaded.items()}
                    self._mtime = mtime
                token = self._cache.get(tenant_id) or self._cache.get("*", "")
                if token:
                    return token
        if self.token_file:
            return Path(self.token_file).read_text(encoding="utf-8").strip()
        return self.token


GRPC_RETRYABLE_CODES = {"UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "ABORTED"}
GRPC_REJECTION_CODES = {"INVALID_ARGUMENT", "FAILED_PRECONDITION", "NOT_FOUND", "OUT_OF_RANGE"}


class GrpcReporter:
    """Reports over the broker's NATIVE listener (default 127.0.0.1:50061).

    The Report* RPCs are `internal_grpc_only`: the broker admits only a loopback
    peer or a peer presenting a verified mTLS client certificate. Plaintext is
    therefore allowed only to a loopback target; any other target requires
    `UDB_EMBEDDING_BROKER_TLS_CERT` + `_KEY` (and normally `_CA`).
    """

    def __init__(self, target: str, *, tls: dict[str, str], tokens: _TokenSource,
                 deadline_seconds: float, retry: RetryPolicy, stop: threading.Event) -> None:
        import grpc  # imported lazily so HTTP mode never needs grpcio

        _ensure_stub_path()
        from udb.core.embedding.services.v1 import embedding_service_pb2 as pb
        from udb.core.embedding.services.v1 import embedding_service_pb2_grpc as pb_grpc

        self._grpc = grpc
        self._pb = pb
        self._deadline = deadline_seconds
        self._retry = retry
        self._stop = stop
        self._tokens = tokens
        if tls.get("cert") or tls.get("ca"):
            def read(path: str) -> bytes | None:
                return Path(path).read_bytes() if path else None

            credentials = grpc.ssl_channel_credentials(
                root_certificates=read(tls.get("ca", "")),
                private_key=read(tls.get("key", "")),
                certificate_chain=read(tls.get("cert", "")),
            )
            options = []
            if tls.get("server_name"):
                options.append(("grpc.ssl_target_name_override", tls["server_name"]))
            self._channel = grpc.secure_channel(target, credentials, options=options)
        else:
            if not is_loopback_target(target):
                raise sidecar.SidecarError(
                    "UDB_EMBEDDING_BROKER_TARGET is not loopback: the Report* RPCs are "
                    "internal_grpc_only, so set UDB_EMBEDDING_BROKER_TLS_CERT/_KEY/_CA "
                    "(verified mTLS client identity) or run the sidecar beside the broker",
                    500,
                )
            self._channel = grpc.insecure_channel(target)
        self._stub = pb_grpc.EmbeddingServiceStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def _metadata(self, tenant_id: str, project_id: str, correlation: str) -> list[tuple[str, str]]:
        metadata = [
            ("x-request-id", str(uuid.uuid4())),
            ("x-correlation-id", correlation),
            ("x-tenant-id", tenant_id),
            ("x-purpose", "embedding-sidecar-consumer"),
        ]
        if project_id:
            metadata.append(("x-project-id", project_id))
        if self._tokens.configured:
            try:
                token = self._tokens.for_tenant(tenant_id)
            except (OSError, ValueError, sidecar.SidecarError) as error:
                # A broken/unmounted token file affects every tenant and may be
                # mid-rotation: transient, rewind rather than drop work.
                raise ReportTransportError(f"bearer token source unreadable: {error}") from error
            if not token:
                raise MissingBearerToken(tenant_id)
            metadata.append(("authorization", f"Bearer {token}"))
        return metadata

    def _call(self, method: Any, request: Any, tenant_id: str, project_id: str, correlation: str) -> Any:
        grpc = self._grpc

        def once() -> Any:
            try:
                # Fresh x-request-id per attempt.
                return method(request, timeout=self._deadline,
                              metadata=self._metadata(tenant_id, project_id, correlation))
            except grpc.RpcError as error:
                code = error.code().name if error.code() else "UNKNOWN"
                if code in GRPC_REJECTION_CODES:
                    raise ReportRejected(f"{code}: {error.details()}") from error
                raise _GrpcFailure(code, error.details() or "") from error

        try:
            return call_with_retry(
                once, self._retry,
                retryable=lambda e: isinstance(e, _GrpcFailure) and e.code in GRPC_RETRYABLE_CODES,
                stop=self._stop,
            )
        except _GrpcFailure as error:
            if error.code in GRPC_RETRYABLE_CODES:
                raise ReportTransportError(f"{error.code}: {error.details}") from error
            raise ReportUndeliverable(tenant_id, f"broker refused report: {error.code}: {error.details}") from error

    def report_batch(self, tenant_id: str, project_id: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        pb = self._pb
        request = pb.ReportEmbeddingBatchRequest(
            tenant_id=tenant_id,
            items=[pb.ReportEmbeddingRequest(**item) for item in items],
            declared_capacity=REPORT_BATCH_LIMIT,
        )
        correlation = f"embedding-batch-{uuid.uuid4()}"
        try:
            response = self._call(self._stub.ReportEmbeddingBatch, request, tenant_id, project_id, correlation)
        except ReportRejected as error:
            return [
                {"work_item_id": item.get("work_item_id", ""), "row_pk": item.get("row_pk", ""),
                 "upserted": False, "error": str(error), "permanent": True}
                for item in items
            ]
        return [
            {"work_item_id": r.work_item_id, "row_pk": r.row_pk, "upserted": r.upserted, "error": r.error}
            for r in response.results
        ]

    def report_failure(self, tenant_id: str, project_id: str, failure: dict[str, Any]) -> None:
        request = self._pb.ReportEmbeddingFailureRequest(**failure)
        try:
            self._call(self._stub.ReportEmbeddingFailure, request, tenant_id, project_id,
                       failure.get("work_item_id", ""))
        except ReportRejected as error:
            # E.g. the work item no longer exists. Nothing more the sidecar can
            # do; the broker's own sweep owns the item from here.
            LOG.error("broker rejected failure report for %s: %s", failure.get("work_item_id"), error)


class _GrpcFailure(Exception):
    def __init__(self, code: str, details: str) -> None:
        super().__init__(f"{code}: {details}")
        self.code = code
        self.details = details


def is_loopback_target(target: str) -> bool:
    host = target
    for prefix in ("dns:///", "ipv4:", "ipv6:"):
        if host.startswith(prefix):
            host = host[len(prefix):]
    if host.startswith("["):
        host = host[1:].split("]", 1)[0]
    elif host.count(":") == 1:
        host = host.rsplit(":", 1)[0]
    return host in {"localhost", "::1"} or host.startswith("127.")


def _ensure_stub_path() -> None:
    """Prefer the installed `udb-client` wheel (which ships the generated
    `udb.*` stubs); fall back to the in-repo `sdk/python/gen` tree."""
    try:
        import udb.core.embedding.services.v1.embedding_service_pb2  # noqa: F401
        return
    except ImportError:
        pass
    candidates = [_env("UDB_PYTHON_STUBS_PATH")]
    candidates.append(str(Path(__file__).resolve().parents[2] / "sdk" / "python" / "gen"))
    for candidate in candidates:
        if candidate and Path(candidate).is_dir() and candidate not in sys.path:
            sys.path.insert(0, candidate)


# ---------------------------------------------------------------------------
# Processing one poll window
# ---------------------------------------------------------------------------


@dataclass
class WindowStats:
    embedded: int = 0
    failed: int = 0
    skipped: int = 0
    unreportable: int = 0
    provider_calls: int = 0


def chunk_ids(chunk: list[tuple[sidecar.WorkItem, list[float]]]) -> list[str]:
    return [work.work_item_id or work.row_pk for work, _ in chunk]


class WorkProcessor:
    def __init__(
        self,
        reporter: Reporter,
        *,
        max_batch_size: int = 64,
        provider_retry: RetryPolicy = RetryPolicy(),
        embed_batch: Callable[[list[sidecar.WorkItem]], list[list[float]]] = sidecar.embed_work_batch,
        sleep: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
        stop: threading.Event | None = None,
    ) -> None:
        self.reporter = reporter
        self.max_batch_size = max(1, min(max_batch_size, REPORT_BATCH_LIMIT))
        self.provider_retry = provider_retry
        self.embed_batch = embed_batch
        self.sleep = sleep
        self.rng = rng
        self.stop = stop

    def process(self, values: list[bytes | str | None]) -> WindowStats:
        """Embed + report every record. Raises ReportTransportError when ANY
        report could not be delivered — the caller must then not commit."""
        stats = WindowStats()
        works: list[sidecar.WorkItem] = []
        for raw in values:
            try:
                works.append(decode_record_value(raw))
            except sidecar.SidecarError as error:
                # Malformed or credential-bearing records are poison: they can
                # never succeed and carry no trustworthy identity to report. The
                # broker's visibility sweep still owns any real work item behind it.
                stats.skipped += 1
                _count("skipped_records")
                LOG.error("skipping undecodable embedding work record: %s", error)
        successes: dict[tuple[str, str], list[tuple[sidecar.WorkItem, list[float]]]] = {}
        failures: list[tuple[sidecar.WorkItem, Exception, bool]] = []
        for batch in group_for_provider(works, self.max_batch_size):
            self._embed(batch, successes, failures, stats)
        for (tenant_id, project_id), done in successes.items():
            for start in range(0, len(done), REPORT_BATCH_LIMIT):
                chunk = done[start : start + REPORT_BATCH_LIMIT]
                try:
                    results = self.reporter.report_batch(
                        tenant_id, project_id, [sidecar.report_from_vector(w, v) for w, v in chunk]
                    )
                except ReportUndeliverable as error:
                    self._undeliverable(error, project_id, chunk_ids(chunk), stats)
                    continue
                by_id = {r.get("work_item_id", ""): r for r in results}
                for work, _ in chunk:
                    result = by_id.get(work.work_item_id) if work.work_item_id else None
                    if result is None and not work.work_item_id:
                        result = next((r for r in results if r.get("row_pk") == work.row_pk), None)
                    if result is not None and result.get("upserted"):
                        stats.embedded += 1
                        continue
                    message = (result or {}).get("error") or "broker did not acknowledge the embedding"
                    failures.append((
                        work,
                        sidecar.SidecarError(f"report rejected: {message}"),
                        not (result or {}).get("permanent", False),
                    ))
        for work, error, retryable in failures:
            stats.failed += 1
            if not work.work_item_id:
                LOG.error("embedding failed for %s/%s without a work_item_id; not reportable: %s",
                          work.source, work.row_pk, error)
                continue
            failure = sidecar.build_failure(work, error, retryable)
            if isinstance(error, sidecar.ProviderError) and error.http_status:
                failure["error_code"] = f"provider_http_{error.http_status}"
            elif str(error).startswith("report rejected"):
                failure["error_code"] = "report_rejected"
            try:
                self.reporter.report_failure(work.tenant_id, work.project_id, failure)
            except ReportUndeliverable as report_error:
                stats.failed -= 1
                self._undeliverable(report_error, work.project_id, [work.work_item_id], stats)
        return stats

    @staticmethod
    def _undeliverable(error: ReportUndeliverable, project_id: str, work_item_ids: list[str],
                       stats: WindowStats) -> None:
        # Never log the token; the tenant and reason only.
        stats.unreportable += len(work_item_ids)
        _count("unreportable_items", len(work_item_ids))
        LOG.error(
            "skipping %d embedding work item(s) for tenant=%s project=%s: %s; committing past "
            "them (the broker's retry sweep re-sends or dead-letters them) work_item_ids=%s",
            len(work_item_ids), error.tenant_id, project_id, error.reason, ",".join(work_item_ids),
        )

    def _embed(self, batch, successes, failures, stats) -> None:
        def run(items: list[sidecar.WorkItem]) -> list[list[float]]:
            stats.provider_calls += 1
            vectors = self.embed_batch(items)
            if len(vectors) != len(items):
                raise sidecar.SidecarError("provider returned a different number of vectors", 502)
            return vectors

        try:
            vectors = call_with_retry(lambda: run(batch), self.provider_retry,
                                      sleep=self.sleep, rng=self.rng, stop=self.stop)
        except Exception as error:  # noqa: BLE001
            retryable = is_retryable(error)
            if not retryable and len(batch) > 1:
                # A permanent error on a multi-item call may be ONE bad input
                # (e.g. over the provider's token limit); isolate it instead of
                # failing its neighbours.
                for work in batch:
                    self._embed([work], successes, failures, stats)
                return
            for work in batch:
                # Retryable-but-exhausted stays retryable: the broker reschedules
                # it within its own attempt budget, then dead-letters.
                failures.append((work, error, retryable))
            return
        for work, vector in zip(batch, vectors):
            successes.setdefault((work.tenant_id, work.project_id), []).append((work, vector))


# ---------------------------------------------------------------------------
# Kafka loop
# ---------------------------------------------------------------------------


class KafkaLike(Protocol):
    def poll(self, timeout_ms: int = 0, max_records: int | None = None) -> dict[Any, list[Any]]: ...
    def commit(self, offsets: dict[Any, Any] | None = None) -> None: ...
    def seek(self, partition: Any, offset: int) -> None: ...
    def close(self) -> None: ...


def default_offset_factory(offset: int) -> Any:
    from kafka.structs import OffsetAndMetadata

    if "leader_epoch" in OffsetAndMetadata._fields:
        return OffsetAndMetadata(offset, "", -1)
    return OffsetAndMetadata(offset, "")  # kafka-python < 2.1


class ConsumerLoop:
    def __init__(
        self,
        consumer: KafkaLike,
        processor: WorkProcessor,
        config: ConsumerConfig,
        *,
        stop: threading.Event,
        offset_factory: Callable[[int], Any] = default_offset_factory,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] | None = None,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self.consumer = consumer
        self.processor = processor
        self.config = config
        self.stop = stop
        self.offset_factory = offset_factory
        self.clock = clock
        self.sleep = sleep or (lambda seconds: stop.wait(seconds))
        self.rng = rng
        self.consecutive_transport_failures = 0

    def collect_window(self) -> list[tuple[Any, Any]]:
        """Poll until `max_batch_size` records or `max_wait_seconds` after the
        first record arrived (or until stop)."""
        window: list[tuple[Any, Any]] = []
        deadline: float | None = None
        while not self.stop.is_set() and len(window) < self.config.max_batch_size:
            if deadline is None:
                timeout = self.config.poll_timeout_seconds
            else:
                timeout = deadline - self.clock()
                if timeout <= 0:
                    break
            polled = self.consumer.poll(
                timeout_ms=max(1, int(timeout * 1000)),
                max_records=self.config.max_batch_size - len(window),
            )
            for partition, records in (polled or {}).items():
                for record in records:
                    window.append((partition, record))
            if window and deadline is None:
                deadline = self.clock() + self.config.max_wait_seconds
            if not window:
                return window  # idle: let run_once re-check stop
        return window

    def run_once(self) -> bool:
        """One window: poll, process, then commit or rewind. Returns True when
        the window was committed."""
        window = self.collect_window()
        if not window:
            return False
        try:
            stats = self.processor.process([record.value for _, record in window])
        except ReportTransportError as error:
            self.consecutive_transport_failures += 1
            self.rewind(window)
            delay = self.config.redelivery_backoff.delay(self.consecutive_transport_failures, self.rng)
            LOG.error("broker report failed; NOT committing, re-delivering %d records after %.1fs: %s",
                      len(window), delay, error)
            self.sleep(delay)
            return False
        self.consecutive_transport_failures = 0
        offsets: dict[Any, Any] = {}
        for partition, record in window:
            next_offset = record.offset + 1
            current = offsets.get(partition)
            if current is None or next_offset > current:
                offsets[partition] = next_offset
        try:
            self.consumer.commit(offsets={p: self.offset_factory(o) for p, o in offsets.items()})
        except Exception as error:  # noqa: BLE001 - e.g. CommitFailedError after a rebalance
            # Reports already landed; the new partition owner re-delivers these
            # records and the broker treats the repeat reports idempotently.
            LOG.warning("offset commit failed (records will be re-delivered): %s", error)
            return False
        LOG.info("window committed records=%d embedded=%d failed=%d skipped=%d unreportable=%d "
                 "provider_calls=%d total_unreportable=%d",
                 len(window), stats.embedded, stats.failed, stats.skipped, stats.unreportable,
                 stats.provider_calls, COUNTERS["unreportable_items"])
        return True

    def rewind(self, window: list[tuple[Any, Any]]) -> None:
        earliest: dict[Any, int] = {}
        for partition, record in window:
            if partition not in earliest or record.offset < earliest[partition]:
                earliest[partition] = record.offset
        for partition, offset in earliest.items():
            self.consumer.seek(partition, offset)

    def run(self) -> None:
        try:
            while not self.stop.is_set():
                self.run_once()
        finally:
            # A window in flight when stop arrives has either been committed or
            # rewound; closing without commit leaves the rest to re-delivery.
            self.consumer.close()


def build_kafka_consumer(config: ConsumerConfig) -> KafkaLike:
    from kafka import KafkaConsumer

    return KafkaConsumer(
        config.topic,
        bootstrap_servers=[s.strip() for s in config.bootstrap_servers.split(",") if s.strip()],
        group_id=config.group_id,
        client_id=config.client_id,
        enable_auto_commit=False,
        auto_offset_reset=config.auto_offset_reset,
        max_poll_records=config.max_batch_size,
        max_poll_interval_ms=config.max_poll_interval_ms,
        session_timeout_ms=config.session_timeout_ms,
        **config.kafka_security,
    )


def build_reporter(stop: threading.Event, retry: RetryPolicy) -> GrpcReporter:
    return GrpcReporter(
        _env("UDB_EMBEDDING_BROKER_TARGET", DEFAULT_BROKER_TARGET),
        tls={
            "ca": _env("UDB_EMBEDDING_BROKER_TLS_CA"),
            "cert": _env("UDB_EMBEDDING_BROKER_TLS_CERT"),
            "key": _env("UDB_EMBEDDING_BROKER_TLS_KEY"),
            "server_name": _env("UDB_EMBEDDING_BROKER_TLS_SERVER_NAME"),
        },
        tokens=_TokenSource(
            tokens_file=_env("UDB_EMBEDDING_BEARER_TOKENS_FILE"),
            token_file=_env("UDB_EMBEDDING_BEARER_TOKEN_FILE"),
            token=_env("UDB_EMBEDDING_BEARER_TOKEN"),
        ),
        deadline_seconds=_env_float("UDB_EMBEDDING_REPORT_DEADLINE_SECONDS", 30.0, 0.1),
        retry=retry,
        stop=stop,
    )


def main() -> None:
    logging.basicConfig(
        level=_env("UDB_EMBEDDING_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = ConsumerConfig.from_env()
    stop = threading.Event()

    def request_stop(signum: int, _frame: Any) -> None:
        LOG.info("signal %s received; finishing the current window and stopping", signum)
        stop.set()

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
    reporter = build_reporter(stop, config.report_retry)
    consumer = build_kafka_consumer(config)
    processor = WorkProcessor(
        reporter,
        max_batch_size=config.max_batch_size,
        provider_retry=config.provider_retry,
        stop=stop,
    )
    LOG.info("consuming topic=%s group=%s bootstrap=%s batch=%d wait=%.3fs",
             config.topic, config.group_id, config.bootstrap_servers,
             config.max_batch_size, config.max_wait_seconds)
    try:
        ConsumerLoop(consumer, processor, config, stop=stop).run()
    finally:
        reporter.close()


if __name__ == "__main__":
    main()
