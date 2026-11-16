"""Bulk writes: ``DataBroker.BulkUpsert`` and the batching loader built on it.

One :meth:`~udb_client.client.UdbClient.bulk_upsert` call is ONE broker transaction:
every row is written or none is, and the broker emits one change event for the
batch. :meth:`~udb_client.client.UdbClient.bulk_load` splits any number of rows into
batches and sends them one after another, stopping at the first failure. It never
retries on its own; give it an ``idempotency_key_prefix`` and a re-run of the same
rows replays the batches that already committed instead of writing them again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from inspect import isawaitable
from typing import Any, Awaitable, Callable, Iterable, Iterator, Mapping, Sequence

from udb.entity.v1 import types_pb2

from .exceptions import UdbError

#: The most rows the broker takes in one batch.
MAX_BULK_ROWS = 10000
DEFAULT_BATCH_ROWS = 1000
DEFAULT_MAX_BATCH_BYTES = 4 * 1024 * 1024

#: Called after each committed batch with its index (from 0) and response.
OnBatch = Callable[[int, Any], Any]


@dataclass
class BulkLoadResult:
    """What a :meth:`bulk_load` committed."""

    #: Batches committed.
    batches: int = 0
    #: Rows sent in the committed batches.
    rows: int = 0
    #: Rows the broker reported written, summed over the committed batches.
    affected_rows: int = 0
    #: Each committed batch's ``BulkUpsertResponse``, in send order.
    responses: list[Any] = field(default_factory=list)


class BulkLoadError(UdbError):
    """A :meth:`bulk_load` stopped before its last batch.

    Every batch before the failing one committed and stays committed; ``result``
    says how many. The failing batch wrote nothing. The cause is chained, so
    :func:`udb_client.error_code.inspect` reads the broker's refusal from this error.
    """

    def __init__(self, batch: int, result: BulkLoadResult, cause: BaseException):
        self.batch = batch
        self.result = result
        self.batches = result.batches
        self.rows = result.rows
        self.affected_rows = result.affected_rows
        self.responses = result.responses
        super().__init__(
            f"udb: bulk load stopped at batch {batch}: {result.batches} batch(es) "
            f"({result.rows} rows) committed before it: {cause}"
        )


def bulk_upsert_request(
    message_type: str,
    records: Sequence[bytes],
    conflict_fields: Sequence[str] = (),
    idempotency_key: str = "",
    batch_id: str = "",
) -> types_pb2.BulkUpsertRequest:
    """The request for one batch of already-encoded rows."""
    if idempotency_key and not idempotency_key.strip():
        raise ValueError("udb: idempotency key must not be whitespace-only")
    return types_pb2.BulkUpsertRequest(
        message_type=message_type,
        records_json=list(records),
        conflict_fields=list(conflict_fields),
        idempotency_key=idempotency_key,
        batch_id=batch_id,
    )


def _check_options(batch_rows: int, max_batch_bytes: int) -> None:
    if not 1 <= batch_rows <= MAX_BULK_ROWS:
        raise ValueError(f"udb: batch_rows must be between 1 and {MAX_BULK_ROWS}, got {batch_rows}")
    if max_batch_bytes <= 0:
        raise ValueError(f"udb: max_batch_bytes must be positive, got {max_batch_bytes}")


def _batches(
    rows: Iterable[Mapping[str, Any]],
    encode: Callable[[Mapping[str, Any]], bytes],
    batch_rows: int,
    max_batch_bytes: int,
) -> Iterator[list[bytes]]:
    """Encoded rows grouped into batches. A batch closes at ``batch_rows`` rows, or
    before a row that would take it past ``max_batch_bytes``. A row larger than
    ``max_batch_bytes`` by itself raises when it is reached."""
    batch: list[bytes] = []
    size = 0
    for index, row in enumerate(rows):
        record = encode(row)
        if len(record) > max_batch_bytes:
            raise ValueError(
                f"udb: row {index} encodes to {len(record)} bytes, more than max_batch_bytes {max_batch_bytes}"
            )
        if batch and (len(batch) >= batch_rows or size + len(record) > max_batch_bytes):
            yield batch
            batch, size = [], 0
        batch.append(record)
        size += len(record)
    if batch:
        yield batch


def _batch_names(index: int, batch_id_prefix: str, idempotency_key_prefix: str) -> tuple[str, str]:
    batch_id = f"{batch_id_prefix}/{index:06d}" if batch_id_prefix else ""
    key = f"{idempotency_key_prefix}-{index}" if idempotency_key_prefix else ""
    return batch_id, key


def _record(result: BulkLoadResult, sent: int, response: Any) -> None:
    result.batches += 1
    result.rows += sent
    result.affected_rows += int(response.affected_rows)
    result.responses.append(response)


def run_bulk_load(
    send: Callable[[list[bytes], str, str], Any],
    rows: Iterable[Mapping[str, Any]],
    encode: Callable[[Mapping[str, Any]], bytes],
    *,
    batch_rows: int,
    max_batch_bytes: int,
    batch_id_prefix: str,
    idempotency_key_prefix: str,
    on_batch: OnBatch | None,
) -> BulkLoadResult:
    """Send ``rows`` batch by batch through ``send(records, batch_id, key)``."""
    _check_options(batch_rows, max_batch_bytes)
    result = BulkLoadResult()
    index = 0
    batches = _batches(rows, encode, batch_rows, max_batch_bytes)
    while True:
        try:
            records = next(batches, None)
            if records is None:
                return result
            batch_id, key = _batch_names(index, batch_id_prefix, idempotency_key_prefix)
            response = send(records, batch_id, key)
        except Exception as error:
            raise BulkLoadError(index, result, error) from error
        _record(result, len(records), response)
        if on_batch is not None:
            on_batch(index, response)
        index += 1


async def run_bulk_load_async(
    send: Callable[[list[bytes], str, str], Awaitable[Any]],
    rows: Iterable[Mapping[str, Any]],
    encode: Callable[[Mapping[str, Any]], bytes],
    *,
    batch_rows: int,
    max_batch_bytes: int,
    batch_id_prefix: str,
    idempotency_key_prefix: str,
    on_batch: OnBatch | None,
) -> BulkLoadResult:
    """:func:`run_bulk_load` for asyncio. ``on_batch`` may be a coroutine function."""
    _check_options(batch_rows, max_batch_bytes)
    result = BulkLoadResult()
    index = 0
    batches = _batches(rows, encode, batch_rows, max_batch_bytes)
    while True:
        try:
            records = next(batches, None)
            if records is None:
                return result
            batch_id, key = _batch_names(index, batch_id_prefix, idempotency_key_prefix)
            response = await send(records, batch_id, key)
        except Exception as error:
            raise BulkLoadError(index, result, error) from error
        _record(result, len(records), response)
        if on_batch is not None:
            outcome = on_batch(index, response)
            if isawaitable(outcome):
                await outcome
        index += 1
