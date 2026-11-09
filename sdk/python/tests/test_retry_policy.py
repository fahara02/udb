import grpc

from udb_client.generated_client import (
    RPC_REPLAY_SAFE,
    RetryPolicy,
    _is_replay_safe,
    _request_has_idempotency_key,
)

_TRANSIENT = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.RESOURCE_EXHAUSTED)


def test_retry_policy_does_not_retry_mutations() -> None:
    # Default (no replay-safe / no idempotency key) — every mutation fails safe.
    policy = RetryPolicy(max_attempts=4)

    for code in (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ):
        assert not policy.should_retry(code, 1, read_only=False)


def test_retry_policy_keeps_read_only_transient_retries() -> None:
    # Read-only retries are unchanged: transient codes + DEADLINE_EXCEEDED retry,
    # and replay-safe / key flags must not alter read-only behavior.
    policy = RetryPolicy(max_attempts=4)

    for code in (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ):
        assert policy.should_retry(code, 1, read_only=True)
        # Flags are ignored for read-only RPCs.
        assert policy.should_retry(
            code, 1, read_only=True, replay_safe=False, has_idempotency_key=False
        )
    assert not policy.should_retry(grpc.StatusCode.INVALID_ARGUMENT, 1, read_only=True)


def test_replay_safe_map_matches_proto_contract() -> None:
    # The proto-derived replay-safety map (R1.1) flags only RPCs whose
    # method_idempotency_contract declares replay_safe=true.
    truthy = {path for path, val in RPC_REPLAY_SAFE.items() if val == "true"}
    assert truthy == {
        "/udb.services.v1.DataBroker/Insert",
        "/udb.services.v1.DataBroker/Upsert",
        "/udb.services.v1.DataBroker/Delete",
        "/udb.services.v1.DataBroker/Update",
        "/udb.core.asset.services.v1.AssetService/StartPipeline",
        "/udb.core.vault.services.v1.VaultService/GenerateDatabaseCredentials",
        "/udb.core.vault.services.v1.VaultService/RevokeDatabaseCredentials",
        "/udb.core.messaging.services.v1.MessagingService/CreateGroup",
        "/udb.core.messaging.services.v1.MessagingService/RegisterMessagingDevice",
        "/udb.core.messaging.services.v1.MessagingService/SendMessage",
        "/udb.core.messaging.services.v1.MessagingService/SubmitCommit",
    }
    assert _is_replay_safe("/udb.services.v1.DataBroker/Upsert") is True
    assert _is_replay_safe("/udb.services.v1.DataBroker/Delete") is True
    assert _is_replay_safe("/udb.services.v1.DataBroker/Update") is True
    assert _is_replay_safe("/udb.core.asset.services.v1.AssetService/StartPipeline") is True
    assert (
        _is_replay_safe(
            "/udb.core.vault.services.v1.VaultService/GenerateDatabaseCredentials"
        )
        is True
    )
    assert (
        _is_replay_safe(
            "/udb.core.vault.services.v1.VaultService/RevokeDatabaseCredentials"
        )
        is True
    )
    assert _is_replay_safe("/udb.services.v1.DataBroker/Select") is False
    assert _is_replay_safe("/unknown/Rpc") is False


def test_replay_safe_mutation_with_key_retries_transient() -> None:
    # Replay-safe mutation + idempotency key → retries on allowed transient codes.
    policy = RetryPolicy(max_attempts=4)
    for code in _TRANSIENT:
        assert policy.should_retry(
            code, 1, read_only=False, replay_safe=True, has_idempotency_key=True
        )


def test_replay_safe_mutation_never_retries_deadline() -> None:
    # Even replay-safe + keyed, a DEADLINE_EXCEEDED leaves the write ambiguous and
    # must NOT auto-retry (only transient codes do).
    policy = RetryPolicy(max_attempts=4)
    assert not policy.should_retry(
        grpc.StatusCode.DEADLINE_EXCEEDED,
        1,
        read_only=False,
        replay_safe=True,
        has_idempotency_key=True,
    )


def test_replay_safe_mutation_without_key_does_not_retry() -> None:
    # Replay-safe but NO idempotency key → fail safe, never retry.
    policy = RetryPolicy(max_attempts=4)
    for code in _TRANSIENT:
        assert not policy.should_retry(
            code, 1, read_only=False, replay_safe=True, has_idempotency_key=False
        )


def test_non_replay_safe_mutation_with_key_does_not_retry() -> None:
    # Non-replay-safe mutation is NEVER retried, even with an idempotency key.
    policy = RetryPolicy(max_attempts=4)
    for code in _TRANSIENT:
        assert not policy.should_retry(
            code, 1, read_only=False, replay_safe=False, has_idempotency_key=True
        )


def test_replay_safe_mutation_stops_at_max_attempts() -> None:
    policy = RetryPolicy(max_attempts=2)
    assert policy.should_retry(
        grpc.StatusCode.UNAVAILABLE,
        1,
        read_only=False,
        replay_safe=True,
        has_idempotency_key=True,
    )
    assert not policy.should_retry(
        grpc.StatusCode.UNAVAILABLE,
        2,
        read_only=False,
        replay_safe=True,
        has_idempotency_key=True,
    )


# ── End-to-end proof through _invoke_unary (path + request introspection) ─────

import pytest  # noqa: E402

from udb_client import UdbRpcError  # noqa: E402
from udb_client.generated_client import DataBrokerClient  # noqa: E402

_relational = pytest.importorskip("udb.entity.v1.relational_pb2")


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return "transient"

    def trailing_metadata(self):
        return ()


class _FakeUnary:
    """A captured-call unary method that raises queued codes, then returns."""

    def __init__(self, response, *, raise_codes=()) -> None:
        self._response = response
        self.calls = 0
        self._raise_codes = list(raise_codes)

    def __call__(self, request, *, metadata=None, timeout=None):
        self.calls += 1
        if self._raise_codes:
            raise _FakeRpcError(self._raise_codes.pop(0))
        return self._response


def _fast_client() -> DataBrokerClient:
    return DataBrokerClient(
        target="unused:1",
        retry=RetryPolicy(max_attempts=4, initial_backoff=0.0, jitter=0.0),
    )


def test_invoke_replay_safe_mutation_with_key_retries_then_succeeds() -> None:
    # Upsert is replay-safe AND we supply an idempotency key → one transient
    # failure is retried, then the call succeeds.
    client = _fast_client()
    try:
        fake = _FakeUnary(None, raise_codes=[grpc.StatusCode.UNAVAILABLE])
        client._stub.Upsert = fake
        req = _relational.UpsertRequest(message_type="Order", idempotency_key="idem-1")
        client.upsert(req)
        assert fake.calls == 2  # one retry then success
    finally:
        client.close()


def test_invoke_replay_safe_mutation_without_key_not_retried() -> None:
    # Upsert is replay-safe but NO idempotency key → fail safe, single attempt.
    client = _fast_client()
    try:
        fake = _FakeUnary(None, raise_codes=[grpc.StatusCode.UNAVAILABLE])
        client._stub.Upsert = fake
        req = _relational.UpsertRequest(message_type="Order")  # no idempotency_key
        with pytest.raises(UdbRpcError):
            client.upsert(req)
        assert fake.calls == 1
    finally:
        client.close()


def test_invoke_replay_safe_mutation_with_blank_key_not_retried() -> None:
    # Whitespace-only keys are broker-keyless, so the SDK retry gate must fail
    # closed before a transient mutation retry can duplicate the write.
    client = _fast_client()
    try:
        fake = _FakeUnary(None, raise_codes=[grpc.StatusCode.UNAVAILABLE])
        client._stub.Upsert = fake
        req = _relational.UpsertRequest(message_type="Order", idempotency_key="   ")
        assert not _request_has_idempotency_key(req)
        with pytest.raises(UdbRpcError):
            client.upsert(req)
        assert fake.calls == 1
    finally:
        client.close()


def test_invoke_replay_safe_mutation_with_context_only_key_not_retried() -> None:
    # The broker's replay contract is the top-level request idempotency_key.
    # RequestContext has no idempotency-key field; a context-only key must not
    # exist or unlock mutation auto-retry.
    client = _fast_client()
    try:
        fake = _FakeUnary(None, raise_codes=[grpc.StatusCode.UNAVAILABLE])
        client._stub.Upsert = fake
        req = _relational.UpsertRequest(message_type="Order")
        with pytest.raises(AttributeError):
            req.context.idempotency_key = "ctx-key"
        assert not _request_has_idempotency_key(req)
        with pytest.raises(UdbRpcError):
            client.upsert(req)
        assert fake.calls == 1
    finally:
        client.close()


def test_invoke_non_replay_safe_mutation_with_key_not_retried() -> None:
    # DocumentUpsert is a mutation but NOT replay-safe → never retried even with
    # an idempotency key supplied.
    client = _fast_client()
    try:
        stores = pytest.importorskip("udb.entity.v1.stores_pb2")
        fake = _FakeUnary(None, raise_codes=[grpc.StatusCode.UNAVAILABLE])
        client._stub.DocumentUpsert = fake
        req = stores.DocumentUpsertRequest(idempotency_key="idem-2")
        with pytest.raises(UdbRpcError):
            client.document_upsert(req)
        assert fake.calls == 1
    finally:
        client.close()


def test_invoke_read_only_unchanged() -> None:
    # Select is read-only → still retried on a transient code (behavior unchanged).
    client = _fast_client()
    try:
        types = pytest.importorskip("udb.entity.v1.types_pb2")
        fake = _FakeUnary(None, raise_codes=[grpc.StatusCode.UNAVAILABLE])
        client._stub.Select = fake
        client.select(types.SelectRequest(message_type="Order"))
        assert fake.calls == 2  # one retry then success
    finally:
        client.close()


# ── Broker retry_after_ms (rate limit / fair admission / overload) ──────────
#
# A RESOURCE_EXHAUSTED refusal carries ``retry_after_ms`` in the
# ``udb-error-detail-bin`` trailer. The retry waits at least that long and never
# sleeps past the call's deadline; a delay longer than ``max_retry_after_ms``
# (default 10 s) is raised to the caller at once.

from udb_client import generated_client as _gc  # noqa: E402
from udb_client.generated_client import UdbDetailedRpcError, invoke_unary  # noqa: E402


class _RateLimitedError(grpc.RpcError):
    def __init__(self, retry_after_ms: int) -> None:
        error_pb2 = pytest.importorskip("udb.entity.v1.error_pb2")
        self._trailer = error_pb2.ErrorDetail(
            retryable=True, retry_after_ms=retry_after_ms
        ).SerializeToString()

    def code(self):
        return grpc.StatusCode.RESOURCE_EXHAUSTED

    def details(self):
        return "rate limited"

    def trailing_metadata(self):
        return (("udb-error-detail-bin", self._trailer),)


class _RateLimitedUnary:
    def __init__(self, failures: int, retry_after_ms: int) -> None:
        self.calls = 0
        self._failures = failures
        self._retry_after_ms = retry_after_ms

    def __call__(self, request, *, metadata=None, timeout=None):
        self.calls += 1
        if self.calls <= self._failures:
            raise _RateLimitedError(self._retry_after_ms)
        return "ok"


def _record_sleeps(monkeypatch) -> list[float]:
    sleeps: list[float] = []
    monkeypatch.setattr(_gc.time, "sleep", sleeps.append)
    return sleeps


def _jittered_client(**retry) -> DataBrokerClient:
    policy = RetryPolicy(max_attempts=4, initial_backoff=0.1, max_backoff=0.8, **retry)
    return DataBrokerClient(target="unused:1", retry=policy)


@pytest.mark.parametrize("run", range(3))
def test_retry_waits_at_least_server_retry_after(monkeypatch, run) -> None:
    sleeps = _record_sleeps(monkeypatch)
    types = pytest.importorskip("udb.entity.v1.types_pb2")
    client = _jittered_client()
    try:
        fake = _RateLimitedUnary(1, 1200)
        client._stub.Select = fake
        assert client.select(types.SelectRequest(message_type="Order")) == "ok"
        assert fake.calls == 2
        assert len(sleeps) == 1 and sleeps[0] >= 1.2, sleeps
    finally:
        client.close()


def test_invoke_unary_waits_at_least_server_retry_after(monkeypatch) -> None:
    sleeps = _record_sleeps(monkeypatch)
    fake = _RateLimitedUnary(1, 1200)
    policy = RetryPolicy(initial_backoff=0.1, max_backoff=0.8)
    result = invoke_unary(
        fake, "Select", object(), call_metadata=(), timeout=None, read_only=True, retry=policy
    )
    assert result == "ok" and fake.calls == 2
    assert len(sleeps) == 1 and sleeps[0] >= 1.2, sleeps


def test_retry_after_past_deadline_raises_without_sleep(monkeypatch) -> None:
    sleeps = _record_sleeps(monkeypatch)
    types = pytest.importorskip("udb.entity.v1.types_pb2")
    client = _jittered_client()
    try:
        fake = _RateLimitedUnary(1, 5000)
        client._stub.Select = fake
        with pytest.raises(UdbDetailedRpcError) as excinfo:
            client.select(types.SelectRequest(message_type="Order"), timeout=1.0)
        assert fake.calls == 1
        assert sleeps == []
        assert excinfo.value.code == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert excinfo.value.retry_after_ms == 5000
    finally:
        client.close()


@pytest.mark.parametrize(
    ("retry_after_ms", "retry"), [(90_000, {"max_retry_after_ms": 2500}), (60_000, {})]
)
def test_retry_after_above_cap_raises_without_sleep(monkeypatch, retry_after_ms, retry) -> None:
    sleeps = _record_sleeps(monkeypatch)
    types = pytest.importorskip("udb.entity.v1.types_pb2")
    client = _jittered_client(**retry)
    try:
        fake = _RateLimitedUnary(1, retry_after_ms)
        client._stub.Select = fake
        with pytest.raises(UdbDetailedRpcError) as excinfo:
            client.select(types.SelectRequest(message_type="Order"), timeout=None)
        assert fake.calls == 1
        assert sleeps == []
        assert excinfo.value.code == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert excinfo.value.retry_after_ms == retry_after_ms
    finally:
        client.close()


def test_retry_policy_default_retry_after_cap() -> None:
    assert RetryPolicy().max_retry_after_ms == 10000
