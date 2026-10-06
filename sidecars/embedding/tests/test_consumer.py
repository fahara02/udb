"""Unit tests for the embedding sidecar's Kafka consumer mode (fakes only)."""

from __future__ import annotations

import json
import threading
from collections import namedtuple
from concurrent import futures
from typing import Any

import pytest

import embedding_consumer as ec
import embedding_sidecar as sidecar

Record = namedtuple("Record", "topic partition offset value")


def work(n: int, **overrides: Any) -> dict[str, Any]:
    base = {
        "work_item_id": f"wi-{n}",
        "tenant_id": "tenant-a",
        "project_id": "project-a",
        "source": "docs",
        "row_pk": f"pk-{n}",
        "text": f"chunk number {n}",
        "model_id": "model-1",
        "model_name": "text-embedding-3-small",
        "provider": "openai",
        "provider_endpoint_ref": "vault://embeddings/openai",
        "dimensions": 3,
        "chunk_hash": f"h{n}",
        "token_count": 3,
    }
    base.update(overrides)
    return base


def envelope(payload: dict[str, Any]) -> bytes:
    """The outbox compliance envelope shape: domain fields nested under payload."""
    return json.dumps({"event_id": "e", "topic": ec.DEFAULT_TOPIC, "payload": {"payload": payload}}).encode()


class FakeReporter:
    def __init__(self, events: list | None = None) -> None:
        self.batches: list[tuple[str, str, list[dict[str, Any]]]] = []
        self.failures: list[tuple[str, str, dict[str, Any]]] = []
        self.events = events if events is not None else []
        self.transport_failures = 0
        self.reject_ids: set[str] = set()

    def report_batch(self, tenant_id, project_id, items):
        if self.transport_failures:
            self.transport_failures -= 1
            raise ec.ReportTransportError("UNAVAILABLE: broker down")
        self.batches.append((tenant_id, project_id, items))
        self.events.append("report")
        return [
            {"work_item_id": i["work_item_id"], "row_pk": i["row_pk"],
             "upserted": i["work_item_id"] not in self.reject_ids,
             "error": "model mismatch" if i["work_item_id"] in self.reject_ids else ""}
            for i in items
        ]

    def report_failure(self, tenant_id, project_id, failure):
        self.failures.append((tenant_id, project_id, failure))
        self.events.append("failure")


def vec_for(items):
    return [[float(index), 0.0, 1.0] for index, _ in enumerate(items)]


FAST = ec.RetryPolicy(max_attempts=4, base_seconds=0.1, cap_seconds=1.0)


def processor(reporter, embed, sleeps=None, max_batch=64):
    return ec.WorkProcessor(
        reporter, max_batch_size=max_batch, provider_retry=FAST, embed_batch=embed,
        sleep=(sleeps.append if sleeps is not None else (lambda _s: None)), rng=lambda: 1.0,
    )


# -- decoding / grouping -----------------------------------------------------


def test_unwrap_nested_and_string_envelopes():
    inner = work(1)
    assert ec.decode_record_value(envelope(inner)).work_item_id == "wi-1"
    stringly = json.dumps({"payload": json.dumps(inner)}).encode()
    assert ec.decode_record_value(stringly).row_pk == "pk-1"
    assert ec.decode_record_value(json.dumps(inner).encode()).model_id == "model-1"


def test_grouping_by_provider_key_and_max_batch_size():
    items = [
        sidecar.work_from_value(work(1)),
        sidecar.work_from_value(work(2, model_name="other-model")),
        sidecar.work_from_value(work(3)),
        sidecar.work_from_value(work(4, tenant_id="tenant-b")),
        sidecar.work_from_value(work(5, dimensions=8)),
        sidecar.work_from_value(work(6)),
        sidecar.work_from_value(work(7, provider_endpoint_ref="vault://other")),
    ]
    batches = ec.group_for_provider(items, max_batch_size=2)
    ids = [[w.work_item_id for w in b] for b in batches]
    assert ids == [["wi-1", "wi-3"], ["wi-6"], ["wi-2"], ["wi-4"], ["wi-5"], ["wi-7"]]


def test_one_provider_call_per_batch_and_results_split_by_index(monkeypatch):
    calls = []
    monkeypatch.setattr(sidecar, "resolve_vault_reference",
                        lambda ref, t, p: {"endpoint": "https://p.example/v1", "api_key": "k"})

    def fake_post(url, body, headers=None):
        calls.append((url, body))
        # Return out of order: the sidecar must reorder by `index`.
        data = [{"index": i, "embedding": [float(i + 1), 0.0, 0.0]} for i in range(len(body["input"]))]
        return {"data": list(reversed(data))}

    monkeypatch.setattr(sidecar, "post_json", fake_post)
    items = [sidecar.work_from_value(work(n)) for n in range(3)]
    vectors = sidecar.embed_work_batch(items)
    assert len(calls) == 1
    assert calls[0][0] == "https://p.example/v1/embeddings"
    assert calls[0][1]["input"] == ["chunk number 0", "chunk number 1", "chunk number 2"]
    assert calls[0][1]["dimensions"] == 3
    assert [v[0] for v in vectors] == [1.0, 1.0, 1.0]  # normalized unit vectors along x
    assert len(vectors) == 3


def test_processor_reports_successes_in_one_batch_per_tenant_project():
    reporter = FakeReporter()
    calls = []

    def embed(items):
        calls.append([w.work_item_id for w in items])
        return vec_for(items)

    stats = processor(reporter, embed).process(
        [envelope(work(1)), envelope(work(2)), envelope(work(3, tenant_id="tenant-b"))]
    )
    assert calls == [["wi-1", "wi-2"], ["wi-3"]]
    assert [(t, p, [i["work_item_id"] for i in items]) for t, p, items in reporter.batches] == [
        ("tenant-a", "project-a", ["wi-1", "wi-2"]),
        ("tenant-b", "project-a", ["wi-3"]),
    ]
    item = reporter.batches[0][2][0]
    assert item["model"] == "model-1" and item["source_name"] == "docs" and item["dims"] == 3
    assert stats.embedded == 3 and stats.failed == 0 and reporter.failures == []


# -- retries -------------------------------------------------------------------


def test_429_is_retried_with_backoff_then_succeeds():
    reporter = FakeReporter()
    sleeps: list[float] = []
    attempts = {"n": 0}

    def embed(items):
        attempts["n"] += 1
        if attempts["n"] <= 2:
            raise sidecar.ProviderError("provider request failed: HTTP 429", 429, True)
        return vec_for(items)

    stats = processor(reporter, embed, sleeps).process([envelope(work(1))])
    assert attempts["n"] == 3
    assert sleeps == [0.1, 0.2]  # exponential (jitter pinned to 1.0)
    assert stats.embedded == 1 and reporter.failures == []


def test_retry_delay_is_jittered_and_capped():
    policy = ec.RetryPolicy(max_attempts=10, base_seconds=1.0, cap_seconds=5.0)
    assert policy.delay(1, lambda: 0.5) == 0.5
    assert policy.delay(3, lambda: 1.0) == 4.0
    assert policy.delay(9, lambda: 1.0) == 5.0
    assert policy.delay(9, lambda: 0.0) == 0.0


def test_exhausted_retryable_error_reports_retryable_failure():
    reporter = FakeReporter()
    sleeps: list[float] = []

    def embed(items):
        raise sidecar.ProviderError("provider request failed: HTTP 503", 503, True)

    stats = processor(reporter, embed, sleeps).process([envelope(work(1))])
    assert len(sleeps) == FAST.max_attempts - 1
    assert stats.failed == 1 and reporter.batches == []
    _, _, failure = reporter.failures[0]
    assert failure["work_item_id"] == "wi-1"
    assert failure["retryable"] is True
    assert failure["error_code"] == "provider_http_503"


def test_non_retryable_error_is_not_retried_and_is_reported():
    reporter = FakeReporter()
    sleeps: list[float] = []
    calls = []

    def embed(items):
        calls.append(len(items))
        raise sidecar.ProviderError("provider request failed: HTTP 400", 400, False)

    stats = processor(reporter, embed, sleeps).process([envelope(work(1))])
    assert calls == [1] and sleeps == []
    assert stats.failed == 1
    tenant, project, failure = reporter.failures[0]
    assert (tenant, project) == ("tenant-a", "project-a")
    assert failure == {
        "tenant_id": "tenant-a", "work_item_id": "wi-1",
        "error": "provider request failed: HTTP 400", "retryable": False,
        "error_code": "provider_http_400",
    }


def test_permanent_batch_error_is_isolated_to_the_bad_item():
    reporter = FakeReporter()

    def embed(items):
        if any(w.work_item_id == "wi-2" for w in items):
            raise sidecar.ProviderError("provider request failed: HTTP 400", 400, False)
        return vec_for(items)

    stats = processor(reporter, embed).process([envelope(work(1)), envelope(work(2)), envelope(work(3))])
    assert stats.embedded == 2 and stats.failed == 1
    assert [f["work_item_id"] for _, _, f in reporter.failures] == ["wi-2"]


def test_broker_rejected_item_becomes_failure_report():
    reporter = FakeReporter()
    reporter.reject_ids = {"wi-2"}
    stats = processor(reporter, vec_for).process([envelope(work(1)), envelope(work(2))])
    assert stats.embedded == 1 and stats.failed == 1
    failure = reporter.failures[0][2]
    assert failure["work_item_id"] == "wi-2" and failure["error_code"] == "report_rejected"
    assert failure["retryable"] is True


def test_credential_bearing_or_garbage_records_are_skipped():
    reporter = FakeReporter()
    stats = processor(reporter, vec_for).process(
        [b"not json", envelope(work(1, api_key="sk-live")), None, envelope(work(2))]
    )
    assert stats.skipped == 3 and stats.embedded == 1


# -- commit ordering -----------------------------------------------------------


class FakeConsumer:
    def __init__(self, records: list[Record], events: list) -> None:
        self.pending = list(records)
        self.events = events
        self.commits: list[dict] = []
        self.seeks: list[tuple] = []
        self.closed = False

    def poll(self, timeout_ms=0, max_records=None):
        batch, self.pending = self.pending[:max_records], self.pending[max_records:]
        out: dict = {}
        for record in batch:
            out.setdefault(("topic", record.partition), []).append(record)
        return out

    def commit(self, offsets=None):
        self.commits.append(dict(offsets))
        self.events.append("commit")

    def seek(self, partition, offset):
        self.seeks.append((partition, offset))
        self.events.append("seek")
        self.pending = [r for r in self._all if r.partition == partition[1] and r.offset >= offset] + [
            r for r in self.pending if r.partition != partition[1]]

    def close(self):
        self.closed = True


def make_loop(records, reporter, events, max_batch=10):
    consumer = FakeConsumer(records, events)
    consumer._all = list(records)
    config = ec.ConsumerConfig(max_batch_size=max_batch, max_wait_seconds=0.0)
    loop = ec.ConsumerLoop(
        consumer, processor(reporter, vec_for, max_batch=max_batch), config,
        stop=threading.Event(), offset_factory=lambda o: o, sleep=lambda _s: None, rng=lambda: 0.0,
    )
    return loop, consumer


def test_commit_happens_only_after_successful_report():
    events: list[str] = []
    reporter = FakeReporter(events)
    records = [Record(ec.DEFAULT_TOPIC, 0, 10, envelope(work(1))),
               Record(ec.DEFAULT_TOPIC, 0, 11, envelope(work(2))),
               Record(ec.DEFAULT_TOPIC, 1, 5, envelope(work(3)))]
    loop, consumer = make_loop(records, reporter, events)
    assert loop.run_once() is True
    assert events == ["report", "commit"]
    assert consumer.commits == [{("topic", 0): 12, ("topic", 1): 6}]


def test_report_transport_failure_rewinds_and_does_not_commit():
    events: list[str] = []
    reporter = FakeReporter(events)
    reporter.transport_failures = 1
    records = [Record(ec.DEFAULT_TOPIC, 0, 10, envelope(work(1))),
               Record(ec.DEFAULT_TOPIC, 0, 11, envelope(work(2)))]
    loop, consumer = make_loop(records, reporter, events)
    assert loop.run_once() is False
    assert consumer.commits == [] and consumer.seeks == [(("topic", 0), 10)]
    # Re-delivered window is reported, then committed.
    assert loop.run_once() is True
    assert events == ["seek", "report", "commit"]
    assert consumer.commits == [{("topic", 0): 12}]


def test_failed_items_are_reported_before_commit():
    events: list[str] = []
    reporter = FakeReporter(events)
    records = [Record(ec.DEFAULT_TOPIC, 0, 0, envelope(work(1)))]
    consumer = FakeConsumer(records, events)
    config = ec.ConsumerConfig(max_batch_size=10, max_wait_seconds=0.0)

    def embed(items):
        raise sidecar.ProviderError("HTTP 401", 401, False)

    loop = ec.ConsumerLoop(consumer, processor(reporter, embed), config, stop=threading.Event(),
                           offset_factory=lambda o: o, sleep=lambda _s: None)
    assert loop.run_once() is True
    assert events == ["failure", "commit"]


# -- transport ---------------------------------------------------------------


@pytest.mark.parametrize("target,expected", [
    ("127.0.0.1:50061", True), ("localhost:50061", True), ("[::1]:50061", True),
    ("dns:///127.0.0.1:50061", True), ("broker.internal:50061", False), ("10.0.0.5:50061", False),
])
def test_loopback_detection(target, expected):
    assert ec.is_loopback_target(target) is expected


def test_plaintext_to_non_loopback_target_is_refused():
    pytest.importorskip("grpc")
    with pytest.raises(sidecar.SidecarError, match="internal_grpc_only"):
        ec.GrpcReporter("broker.internal:50061", tls={}, tokens=ec._TokenSource(),
                        deadline_seconds=1, retry=FAST, stop=threading.Event())


def test_tokens_file_selects_per_tenant_bearer(tmp_path):
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps({"tenant-a": "jwt-a", "*": "jwt-default"}))
    tokens = ec._TokenSource(tokens_file=str(path))
    assert tokens.for_tenant("tenant-a") == "jwt-a"
    assert tokens.for_tenant("tenant-z") == "jwt-default"


def test_grpc_reporter_against_in_process_broker():
    grpc = pytest.importorskip("grpc")
    pb = pytest.importorskip("udb.core.embedding.services.v1.embedding_service_pb2")
    pb_grpc = pytest.importorskip("udb.core.embedding.services.v1.embedding_service_pb2_grpc")
    seen: list[tuple[str, dict, Any]] = []

    class Servicer(pb_grpc.EmbeddingServiceServicer):
        def ReportEmbeddingBatch(self, request, context):
            seen.append(("batch", dict(context.invocation_metadata()), request))
            return pb.ReportEmbeddingBatchResponse(
                results=[pb.ReportEmbeddingBatchItemResult(work_item_id=i.work_item_id, row_pk=i.row_pk, upserted=True)
                         for i in request.items], upserted=len(request.items))

        def ReportEmbeddingFailure(self, request, context):
            seen.append(("failure", dict(context.invocation_metadata()), request))
            return pb.ReportEmbeddingFailureResponse(recorded=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    pb_grpc.add_EmbeddingServiceServicer_to_server(Servicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        reporter = ec.GrpcReporter(f"127.0.0.1:{port}", tls={}, tokens=ec._TokenSource(token="svc-jwt"),
                                   deadline_seconds=5, retry=FAST, stop=threading.Event())
        report = sidecar.report_from_vector(sidecar.work_from_value(work(1)), [0.1, 0.2, 0.3])
        results = reporter.report_batch("tenant-a", "project-a", [report])
        reporter.report_failure("tenant-a", "project-a", {
            "tenant_id": "tenant-a", "work_item_id": "wi-2", "error": "boom",
            "retryable": False, "error_code": "provider_http_400"})
        reporter.close()
    finally:
        server.stop(None)
    assert results[0]["upserted"] is True
    kind, metadata, request = seen[0]
    assert kind == "batch" and request.tenant_id == "tenant-a" and request.declared_capacity == 256
    assert list(request.items[0].vector) == pytest.approx([0.1, 0.2, 0.3])
    assert request.items[0].work_item_id == "wi-1" and request.items[0].chunk_hash == "h1"
    assert metadata["x-tenant-id"] == "tenant-a" and metadata["x-project-id"] == "project-a"
    assert metadata["authorization"] == "Bearer svc-jwt"
    assert seen[1][0] == "failure" and seen[1][2].error_code == "provider_http_400"
    assert metadata["x-request-id"] != seen[1][1]["x-request-id"]


# -- per-tenant credentials never stall a partition --------------------------


def _in_process_broker(refuse: dict[str, str] | None = None):
    grpc = pytest.importorskip("grpc")
    pb = pytest.importorskip("udb.core.embedding.services.v1.embedding_service_pb2")
    pb_grpc = pytest.importorskip("udb.core.embedding.services.v1.embedding_service_pb2_grpc")
    seen: list[tuple[str, dict, Any]] = []
    refuse = refuse or {}

    class Servicer(pb_grpc.EmbeddingServiceServicer):
        def ReportEmbeddingBatch(self, request, context):
            metadata = dict(context.invocation_metadata())
            code = refuse.get(request.tenant_id)
            if code:
                context.abort(getattr(grpc.StatusCode, code), "refused")
            seen.append(("batch", metadata, request))
            return pb.ReportEmbeddingBatchResponse(
                results=[pb.ReportEmbeddingBatchItemResult(work_item_id=i.work_item_id, row_pk=i.row_pk, upserted=True)
                         for i in request.items], upserted=len(request.items))

        def ReportEmbeddingFailure(self, request, context):
            seen.append(("failure", dict(context.invocation_metadata()), request))
            return pb.ReportEmbeddingFailureResponse(recorded=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    pb_grpc.add_EmbeddingServiceServicer_to_server(Servicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port, seen


def _grpc_loop(port, tokens, records, events):
    reporter = ec.GrpcReporter(f"127.0.0.1:{port}", tls={}, tokens=tokens, deadline_seconds=5,
                               retry=ec.RetryPolicy(max_attempts=2, base_seconds=0.0, cap_seconds=0.0),
                               stop=threading.Event())
    consumer = FakeConsumer(records, events)
    consumer._all = list(records)
    loop = ec.ConsumerLoop(consumer, processor(reporter, vec_for),
                           ec.ConsumerConfig(max_batch_size=10, max_wait_seconds=0.0),
                           stop=threading.Event(), offset_factory=lambda o: o,
                           sleep=lambda _s: None, rng=lambda: 0.0)
    return loop, consumer, reporter


def test_tenant_without_token_is_skipped_and_offset_advances(tmp_path, caplog):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({"tenant-a": "jwt-a-secret"}))
    server, port, seen = _in_process_broker()
    events: list[str] = []
    records = [Record(ec.DEFAULT_TOPIC, 0, 20, envelope(work(1))),
               Record(ec.DEFAULT_TOPIC, 0, 21, envelope(work(2, tenant_id="tenant-b"))),
               Record(ec.DEFAULT_TOPIC, 0, 22, envelope(work(3))),
               Record(ec.DEFAULT_TOPIC, 1, 7, envelope(work(4, tenant_id="tenant-b")))]
    before = ec.COUNTERS["unreportable_items"]
    try:
        loop, consumer, reporter = _grpc_loop(port, ec._TokenSource(tokens_file=str(tokens_file)), records, events)
        with caplog.at_level("ERROR", logger="udb.embedding.consumer"):
            assert loop.run_once() is True
        reporter.close()
    finally:
        server.stop(None)
    # Tenant A's reports were delivered with A's bearer...
    assert [(k, r.tenant_id, [i.work_item_id for i in r.items]) for k, _, r in seen] == [
        ("batch", "tenant-a", ["wi-1", "wi-3"])]
    assert seen[0][1]["authorization"] == "Bearer jwt-a-secret"
    # ...B never reached the broker, and the offset advanced past B's items.
    assert consumer.seeks == []
    assert consumer.commits == [{("topic", 0): 23, ("topic", 1): 8}]
    assert ec.COUNTERS["unreportable_items"] - before == 2
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "tenant=tenant-b" in logged and "no bearer token" in logged
    assert "jwt-a-secret" not in logged


def test_failure_report_for_tenant_without_token_is_skipped(tmp_path):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({"tenant-a": "jwt-a"}))
    server, port, seen = _in_process_broker()
    events: list[str] = []
    records = [Record(ec.DEFAULT_TOPIC, 0, 0, envelope(work(1, tenant_id="tenant-b")))]
    try:
        reporter = ec.GrpcReporter(f"127.0.0.1:{port}", tls={},
                                   tokens=ec._TokenSource(tokens_file=str(tokens_file)),
                                   deadline_seconds=5, retry=FAST, stop=threading.Event())
        consumer = FakeConsumer(records, events)

        def embed(items):
            raise sidecar.ProviderError("HTTP 400", 400, False)

        loop = ec.ConsumerLoop(consumer, processor(reporter, embed),
                               ec.ConsumerConfig(max_batch_size=10, max_wait_seconds=0.0),
                               stop=threading.Event(), offset_factory=lambda o: o, sleep=lambda _s: None)
        assert loop.run_once() is True
        reporter.close()
    finally:
        server.stop(None)
    assert seen == [] and consumer.commits == [{("topic", 0): 1}]


def test_non_transient_broker_refusal_skips_but_unavailable_rewinds():
    server, port, seen = _in_process_broker(refuse={"tenant-b": "PERMISSION_DENIED", "tenant-c": "UNAVAILABLE"})
    try:
        events: list[str] = []
        records = [Record(ec.DEFAULT_TOPIC, 0, 0, envelope(work(1))),
                   Record(ec.DEFAULT_TOPIC, 0, 1, envelope(work(2, tenant_id="tenant-b")))]
        loop, consumer, reporter = _grpc_loop(port, ec._TokenSource(token="jwt"), records, events)
        assert loop.run_once() is True
        assert consumer.commits == [{("topic", 0): 2}] and consumer.seeks == []
        reporter.close()

        events = []
        records = [Record(ec.DEFAULT_TOPIC, 0, 5, envelope(work(3, tenant_id="tenant-c")))]
        loop, consumer, reporter = _grpc_loop(port, ec._TokenSource(token="jwt"), records, events)
        assert loop.run_once() is False
        assert consumer.commits == [] and consumer.seeks == [(("topic", 0), 5)]
        reporter.close()
    finally:
        server.stop(None)
    assert [r.tenant_id for _, _, r in seen] == ["tenant-a"]
