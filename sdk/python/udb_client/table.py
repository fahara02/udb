"""Typed entity tables, used by the ``udb sdk generate --project-proto`` output.

A generated ``XTable(db)`` reads and writes one entity through the DataBroker with
dict rows keyed by proto field name. :class:`AsyncTable` and :class:`AsyncRepo` are the
same stores for asyncio. After a write, the next reads through the same client carry a
read fence from that write's receipt, so they see it even from a replica. A NULL column is absent from a returned row,
and a value of ``None`` in a write writes SQL NULL, so ``patch(key, {"col": None})``
clears a column. Conflicts and missing rows are exceptions you can catch:
:class:`~udb_client.exceptions.UdbConflict` and
:class:`~udb_client.exceptions.UdbNotFound`.

The broker fills in the verified tenant and stays the only authority: nothing here
sets a principal, actor or scope.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from inspect import isawaitable
import json
from typing import Any, Callable, Generic, Mapping, Sequence, TypeVar

from google.protobuf.descriptor import FieldDescriptor

from udb.entity.v1 import types_pb2

from .client import decode_records, to_record_json, to_struct
from .error_code import classify
from .exceptions import UdbConflict, UdbNotFound, UdbRpcError
from .record_codec import (
    _INT64_TYPES,
    _is_json,
    _text_column,
    enum_common_prefix,
    is_repeated,
    json_column_text,
)
from .schema import column_options, primary_keys, project_column, tenant_column

Row = TypeVar("Row", bound=Mapping[str, Any])
Filter = Mapping[str, Any]

_EXACT_FILTER_NUMBER = 2**53
# Columns an update leaves out when the spec does not name them: the broker refuses
# a write to the row's tenant or project.
_SCOPE_COLUMNS = ("tenant_id", "_tenant_id", "org_id", "institution_id", "project_id", "_project_id")


@dataclass(frozen=True)
class UDBColumn:
    """The column policy the generator emits for each column of an entity."""

    sql_type: str = ""
    not_null: bool = False
    has_presence: bool = False
    is_array: bool = False
    json: bool = False
    jsonb: bool = False
    exclude_from_insert: bool = False
    pii: bool = False
    encrypted: bool = False
    #: The filterable HMAC sibling column; empty when there is none.
    blind_index: str = ""
    enum_tokens: tuple[str, ...] = ()
    #: False for a broker-injected audit column, which the broker fills in.
    declared_in_proto: bool = True


@dataclass(frozen=True)
class TableSpec(Generic[Row]):
    """The per-entity facts the generator emits for a :class:`Table`."""

    message_type: str
    key: tuple[str, ...]
    fields: tuple[str, ...]
    to_record: Callable[[Mapping[str, Any]], dict[str, Any]]
    to_fields: Callable[[Mapping[str, Any]], dict[str, Any]]
    from_row: Callable[[Mapping[str, Any]], Row]
    #: Columns ``update_if`` and ``update_with_retry`` never write, besides the key.
    #: Empty means the conventional tenant and project column names.
    update_excluded: tuple[str, ...] = ()


@dataclass
class TablePage(Generic[Row]):
    """One page of typed rows."""

    rows: list[Row] = field(default_factory=list)
    #: True when the page is full: more rows may match.
    has_more: bool = False
    #: Pass as ``page_token`` to continue; empty on the last page.
    next_page_token: str = ""
    #: Every matching row, when ``include_total`` was set.
    total: int = 0
    #: Columns that came back redacted because the caller lacks the PII read
    #: scope. Never write those values back.
    redacted_fields: list[str] = field(default_factory=list)


class _TableBase(Generic[Row]):
    """Request building and row logic shared by :class:`Table` and :class:`AsyncTable`.

    Everything here is free of I/O; the two subclasses only issue the RPCs.
    """

    def __init__(self, db: Any, spec: TableSpec[Row]):
        self._client = getattr(db, "data", db)
        self._spec = spec

    @property
    def message_type(self) -> str:
        """The entity's udb message type (its proto full name)."""
        return self._spec.message_type

    def key_of(self, row: Mapping[str, Any]) -> dict[str, Any]:
        """The primary key of ``row``. Raises when a key column has no value."""
        key: dict[str, Any] = {}
        for column in self._spec.key:
            value = row.get(column)
            if value is None or value == "":
                raise ValueError(
                    f"udb: {self._spec.message_type} has no value for primary-key column {column}"
                )
            key[column] = value
        if not key:
            raise ValueError(f"udb: {self._spec.message_type} declares no primary key")
        return key

    def _select_request(
        self,
        where: Filter | None,
        limit: int,
        page_token: str,
        order_by: Sequence[str],
        include_total: bool,
        fields: Sequence[str],
        include_pii: bool,
    ) -> types_pb2.SelectRequest:
        sort = []
        for column in order_by:
            descending = column.startswith("-")
            name = column[1:] if descending else column
            self._field(name)
            sort.append(types_pb2.Sort(field=name, descending=descending))
        for column in fields:
            self._field(column)
        request = types_pb2.SelectRequest(
            message_type=self._spec.message_type,
            filter=to_struct(self._fields_of(where or {})),
            limit=limit,
            page_token=page_token,
            sort=sort,
            include_total=include_total,
            fields=list(fields) or (list(self._spec.fields) if include_pii else []),
        )
        # Read-your-writes: the fence of this client's latest write rides on reads.
        fence = self._client.read_fence()
        if fence is not None:
            request.context.CopyFrom(fence)
        return request

    def _page_from(self, response: types_pb2.RecordSet) -> TablePage[Row]:
        return TablePage(
            rows=[self._spec.from_row(row) for row in decode_records(response)],
            has_more=response.has_more,
            next_page_token=response.next_page_token,
            total=response.exact_total,
            redacted_fields=list(response.redacted_fields),
        )

    def _single(self, page: TablePage[Row], key: Mapping[str, Any]) -> Row:
        if not page.rows:
            raise UdbNotFound(f"udb: not found: {self._spec.message_type} {dict(key)}")
        if len(page.rows) > 1:
            raise ValueError(
                f"udb: {self._spec.message_type} key {dict(key)} matched more than one row; "
                "address a primary or unique key"
            )
        return page.rows[0]

    def _write_request(self, row: Mapping[str, Any], idempotency_key: str) -> types_pb2.UpsertRequest:
        self._fields_of(row)  # An unknown column is refused, not dropped.
        return types_pb2.UpsertRequest(
            message_type=self._spec.message_type,
            record_json=to_record_json(self._spec.to_record(row)),
            idempotency_key=idempotency_key,
        )

    def _after_write(self, response: Any) -> None:
        if response.affected_rows == 0:
            raise UdbNotFound(f"udb: not found: {self._spec.message_type} was left untouched")
        self._client.remember_write(response)

    def _patch_fields(self, changes: Mapping[str, Any]) -> dict[str, Any]:
        fields = self._fields_of(changes)
        if not fields:
            raise ValueError(f"udb: patch on {self._spec.message_type} has no columns to change")
        return fields

    def _update_request(
        self,
        where: Mapping[str, Any],
        changes: Mapping[str, Any] | None,
        expected: Mapping[str, Any] | None,
        deltas: Mapping[str, float] | None,
        require_affected: int,
        idempotency_key: str,
    ) -> types_pb2.UpdateRequest:
        request = types_pb2.UpdateRequest(
            message_type=self._spec.message_type,
            filter=to_struct(where),
            require_affected=require_affected,
            idempotency_key=idempotency_key,
        )
        if changes:
            request.changes.CopyFrom(to_struct(changes))
        if expected:
            request.expected.CopyFrom(to_struct(expected))
        for column in sorted(deltas or {}):
            request.increments.add(column=self._field(column), delta=float(deltas[column]))  # type: ignore[index]
        return request

    def _delete_request(
        self,
        where: Mapping[str, Any],
        expected: Mapping[str, Any] | None,
        require_affected: int,
        idempotency_key: str,
    ) -> types_pb2.DeleteRequest:
        request = types_pb2.DeleteRequest(
            message_type=self._spec.message_type,
            filter=to_struct(where),
            require_affected=require_affected,
            idempotency_key=idempotency_key,
        )
        if expected:
            request.expected.CopyFrom(to_struct(expected))
        return request

    def _update_if_request(
        self, next_row: Mapping[str, Any], expected: Mapping[str, Any], idempotency_key: str
    ) -> types_pb2.UpdateRequest:
        return self._update_request(
            self._fields_of(self.key_of(next_row)),
            self._updatable(self._fields_of(next_row)),
            self._fields_of(expected),
            None,
            1,
            idempotency_key,
        )

    def _retry_step(
        self, current: Row, after_row: Row
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """The columns ``after_row`` changed and their previous values."""
        before = self._updatable(self._spec.to_fields(current))
        after = self._updatable(self._spec.to_fields(after_row))
        changes: dict[str, Any] = {}
        expected: dict[str, Any] = {}
        for column in {*before, *after}:
            if json.dumps(before.get(column), sort_keys=True) != json.dumps(
                after.get(column), sort_keys=True
            ):
                changes[column] = after.get(column)
                expected[column] = before.get(column)
        return changes, expected

    def _gave_up(self, attempts: int) -> UdbConflict:
        return UdbConflict(f"udb: conflict: gave up after {attempts} attempts")

    def _updatable(self, fields: Mapping[str, Any]) -> dict[str, Any]:
        """Leave out what an update must never set: the key, and the tenant, project
        and other columns the spec names as owned by the broker."""
        excluded = set(self._spec.key) | set(self._spec.update_excluded or _SCOPE_COLUMNS)
        return {name: value for name, value in fields.items() if name not in excluded}

    def _field(self, name: str) -> str:
        if name not in self._spec.fields:
            raise ValueError(f"udb: {self._spec.message_type} has no column {name!r}")
        return name

    def _fields_of(self, values: Filter) -> dict[str, Any]:
        """Check names and convert values. A column the entity does not have is
        refused: dropping it would widen a filter to rows the caller did not ask for."""
        out: dict[str, Any] = {}
        for name, value in values.items():
            if name in ("$and", "$or"):
                out[name] = [self._fields_of(part) for part in value]
            elif name == "$not":
                out[name] = self._fields_of(value)
            else:
                self._field(name)
                out[name] = self._spec.to_fields({name: value}).get(name, value)
        return out


class Table(_TableBase[Row]):
    """The typed store for one entity. ``db`` is a ``UdbProject`` or ``UdbClient``."""

    def get(self, key: Mapping[str, Any], *, include_pii: bool = False) -> Row:
        """The row with ``key``. A missing row raises :class:`UdbNotFound`."""
        return self._single(self.select(key, limit=2, include_pii=include_pii), key)

    def select(
        self,
        where: Filter | None = None,
        *,
        limit: int = 0,
        page_token: str = "",
        order_by: Sequence[str] = (),
        include_total: bool = False,
        fields: Sequence[str] = (),
        include_pii: bool = False,
    ) -> TablePage[Row]:
        """One page of rows matching ``where``.

        ``order_by`` sorts by columns (``"-column"`` for descending); the primary
        key always breaks ties. ``fields`` reads only those columns. By default
        PII columns are left out; ``include_pii`` asks for them.
        """
        request = self._select_request(where, limit, page_token, order_by, include_total, fields, include_pii)
        return self._page_from(self._call("Select", request))

    def select_all(self, where: Filter | None, fn: Callable[[Row], None]) -> None:
        """Walk every row matching ``where``, page by page, calling ``fn`` for each.
        An exception from ``fn`` stops the walk and propagates."""
        page_token = ""
        while True:
            page = self.select(where, limit=500, page_token=page_token)
            for row in page.rows:
                fn(row)
            if not page.next_page_token or not page.has_more:
                return
            page_token = page.next_page_token

    def count(self, where: Filter | None = None) -> int:
        """The number of rows matching ``where``."""
        return self.select(where, limit=1, include_total=True).total

    def upsert(self, row: Mapping[str, Any], *, idempotency_key: str = "") -> None:
        """Write ``row``: inserted when its key is new, updated when it exists."""
        self._after_write(self._call("Upsert", self._write_request(row, idempotency_key)))

    def insert(self, row: Mapping[str, Any], *, idempotency_key: str = "") -> None:
        """Create ``row``. A key collision raises and never replaces the row."""
        self._after_write(self._call("Insert", self._write_request(row, idempotency_key)))

    def patch(
        self, key: Mapping[str, Any], changes: Mapping[str, Any], *, idempotency_key: str = ""
    ) -> None:
        """Set only the given columns; ``None`` clears a column. A missing row
        raises :class:`UdbNotFound` and nothing is written."""
        request = self._update_request(
            self._fields_of(key), self._patch_fields(changes), None, None, 1, idempotency_key
        )
        self._client.remember_write(self._call("Update", request))

    def update_if(
        self, next_row: Mapping[str, Any], expected: Mapping[str, Any], *, idempotency_key: str = ""
    ) -> None:
        """Write the updatable columns of ``next_row`` only if the stored row still
        holds ``expected``. A lost race raises :class:`UdbConflict`."""
        request = self._update_if_request(next_row, expected, idempotency_key)
        self._client.remember_write(self._call("Update", request))

    def update_with_retry(
        self,
        key: Mapping[str, Any],
        change: Callable[[Row], Row],
        attempts: int = 5,
        *,
        idempotency_key: str = "",
    ) -> Row:
        """Read the row, apply ``change``, and write back only the columns it
        modified, guarded by their previous values. A conflict reads and tries
        again, up to ``attempts`` times. Returns the row as written."""
        attempts = attempts if attempts > 0 else 5
        for _ in range(attempts):
            current = self.get(key)
            after_row = change(copy.deepcopy(current))
            changes, expected = self._retry_step(current, after_row)
            if not changes:
                return current
            request = self._update_request(
                self._fields_of(key), changes, expected, None, 1, idempotency_key
            )
            try:
                self._client.remember_write(self._call("Update", request))
                return after_row
            except UdbConflict:
                continue
        raise self._gave_up(attempts)

    def increment(
        self, key: Mapping[str, Any], deltas: Mapping[str, float], *, idempotency_key: str = ""
    ) -> None:
        """Add ``deltas`` to numeric columns, atomically on the broker, with no
        read-modify-write window."""
        request = self._update_request(self._fields_of(key), None, None, deltas, 1, idempotency_key)
        self._client.remember_write(self._call("Update", request))

    def delete(self, key: Mapping[str, Any], *, idempotency_key: str = "") -> None:
        """Remove the row with ``key``. A missing row raises :class:`UdbNotFound`."""
        request = self._delete_request(self._fields_of(key), None, 1, idempotency_key)
        self._client.remember_write(self._call("Delete", request))

    # The filter-first verbs behind :class:`Repo`.

    def _guarded_update(
        self, where: Filter, changes: Mapping[str, Any], expected: Mapping[str, Any], key: str
    ) -> None:
        request = self._update_request(
            self._fields_of(where), self._fields_of(changes), self._fields_of(expected), None, 0, key
        )
        self._client.remember_write(self._call("Update", request))

    def _guarded_delete(self, where: Filter, expected: Mapping[str, Any], key: str) -> None:
        request = self._delete_request(self._fields_of(where), self._fields_of(expected), 0, key)
        self._client.remember_write(self._call("Delete", request))

    def _call(self, rpc: str, request: Any) -> Any:
        try:
            return self._client.call(rpc, request)
        except UdbRpcError as error:
            classified = classify(error)
            if classified is error:
                raise
            raise classified from error


class AsyncTable(_TableBase[Row]):
    """:class:`Table` for asyncio. ``db`` is a ``UdbAsyncProject`` or ``UdbAsyncClient``."""

    async def get(self, key: Mapping[str, Any], *, include_pii: bool = False) -> Row:
        """See :meth:`Table.get`."""
        return self._single(await self.select(key, limit=2, include_pii=include_pii), key)

    async def select(
        self,
        where: Filter | None = None,
        *,
        limit: int = 0,
        page_token: str = "",
        order_by: Sequence[str] = (),
        include_total: bool = False,
        fields: Sequence[str] = (),
        include_pii: bool = False,
    ) -> TablePage[Row]:
        """See :meth:`Table.select`."""
        request = self._select_request(where, limit, page_token, order_by, include_total, fields, include_pii)
        return self._page_from(await self._call("Select", request))

    async def select_all(self, where: Filter | None, fn: Callable[[Row], Any]) -> None:
        """See :meth:`Table.select_all`; ``fn`` may be sync or async."""
        page_token = ""
        while True:
            page = await self.select(where, limit=500, page_token=page_token)
            for row in page.rows:
                outcome = fn(row)
                if isawaitable(outcome):
                    await outcome
            if not page.next_page_token or not page.has_more:
                return
            page_token = page.next_page_token

    async def count(self, where: Filter | None = None) -> int:
        """See :meth:`Table.count`."""
        return (await self.select(where, limit=1, include_total=True)).total

    async def upsert(self, row: Mapping[str, Any], *, idempotency_key: str = "") -> None:
        """See :meth:`Table.upsert`."""
        self._after_write(await self._call("Upsert", self._write_request(row, idempotency_key)))

    async def insert(self, row: Mapping[str, Any], *, idempotency_key: str = "") -> None:
        """See :meth:`Table.insert`."""
        self._after_write(await self._call("Insert", self._write_request(row, idempotency_key)))

    async def patch(
        self, key: Mapping[str, Any], changes: Mapping[str, Any], *, idempotency_key: str = ""
    ) -> None:
        """See :meth:`Table.patch`."""
        request = self._update_request(
            self._fields_of(key), self._patch_fields(changes), None, None, 1, idempotency_key
        )
        self._client.remember_write(await self._call("Update", request))

    async def update_if(
        self, next_row: Mapping[str, Any], expected: Mapping[str, Any], *, idempotency_key: str = ""
    ) -> None:
        """See :meth:`Table.update_if`."""
        request = self._update_if_request(next_row, expected, idempotency_key)
        self._client.remember_write(await self._call("Update", request))

    async def update_with_retry(
        self,
        key: Mapping[str, Any],
        change: Callable[[Row], Any],
        attempts: int = 5,
        *,
        idempotency_key: str = "",
    ) -> Row:
        """See :meth:`Table.update_with_retry`; ``change`` may be sync or async."""
        attempts = attempts if attempts > 0 else 5
        for _ in range(attempts):
            current = await self.get(key)
            after_row = change(copy.deepcopy(current))
            if isawaitable(after_row):
                after_row = await after_row
            changes, expected = self._retry_step(current, after_row)
            if not changes:
                return current
            request = self._update_request(
                self._fields_of(key), changes, expected, None, 1, idempotency_key
            )
            try:
                self._client.remember_write(await self._call("Update", request))
                return after_row
            except UdbConflict:
                continue
        raise self._gave_up(attempts)

    async def increment(
        self, key: Mapping[str, Any], deltas: Mapping[str, float], *, idempotency_key: str = ""
    ) -> None:
        """See :meth:`Table.increment`."""
        request = self._update_request(self._fields_of(key), None, None, deltas, 1, idempotency_key)
        self._client.remember_write(await self._call("Update", request))

    async def delete(self, key: Mapping[str, Any], *, idempotency_key: str = "") -> None:
        """See :meth:`Table.delete`."""
        request = self._delete_request(self._fields_of(key), None, 1, idempotency_key)
        self._client.remember_write(await self._call("Delete", request))

    async def _guarded_update(
        self, where: Filter, changes: Mapping[str, Any], expected: Mapping[str, Any], key: str
    ) -> None:
        request = self._update_request(
            self._fields_of(where), self._fields_of(changes), self._fields_of(expected), None, 0, key
        )
        self._client.remember_write(await self._call("Update", request))

    async def _guarded_delete(self, where: Filter, expected: Mapping[str, Any], key: str) -> None:
        request = self._delete_request(self._fields_of(where), self._fields_of(expected), 0, key)
        self._client.remember_write(await self._call("Delete", request))

    async def _call(self, rpc: str, request: Any) -> Any:
        try:
            return await self._client.call(rpc, request)
        except UdbRpcError as error:
            classified = classify(error)
            if classified is error:
                raise
            raise classified from error


class _RepoBase(Generic[Row]):
    """Precondition checks shared by :class:`Repo` and :class:`AsyncRepo`."""

    _spec: TableSpec[Row]

    def _check_full_key(self, where: Filter) -> None:
        for column in self._spec.key:
            if column not in where:
                raise ValueError(
                    f"{self._spec.message_type}: primary key {column!r} missing from filter; "
                    "use find_first for a partial-key lookup"
                )

    def _at_most_one(self, rows: Sequence[Row]) -> Row | None:
        if len(rows) > 1:
            raise ValueError(
                f"{self._spec.message_type}: primary key matched {len(rows)} rows, expected at most 1"
            )
        return rows[0] if rows else None

    def _check_expected(self, verb: str, expected: Mapping[str, Any]) -> None:
        if not expected:
            noun = verb.split("_")[0]
            raise ValueError(
                f"{self._spec.message_type}.{verb}: expected precondition is empty; "
                f"refusing to run an unconditional {noun}"
            )


class Repo(_RepoBase[Row]):
    """Filter-first access to one entity: paged lists, a point read that returns
    ``None`` when absent, and guarded writes that refuse to run without a precondition."""

    def __init__(self, db: Any, spec: TableSpec[Row]):
        self._table: Table[Row] = Table(db, spec)
        self._spec = spec

    def list(self, where: Filter | None = None, **options: Any) -> TablePage[Row]:
        """One page of rows matching ``where``. A row that fails to decode fails the page."""
        return self._table.select(where, **options)

    def get(self, where: Filter, *, include_pii: bool = False) -> Row | None:
        """The row identified by its FULL primary key, or ``None`` when absent. A
        partial key is refused before the RPC: use :meth:`find_first` for other lookups."""
        self._check_full_key(where)
        return self._at_most_one(self._table.select(where, limit=2, include_pii=include_pii).rows)

    def find_first(self, where: Filter, *, include_pii: bool = False) -> Row | None:
        """The first row matching an arbitrary filter, or ``None`` when none match."""
        rows = self._table.select(where, limit=1, include_pii=include_pii).rows
        return rows[0] if rows else None

    def update_guarded(
        self,
        where: Filter,
        changes: Mapping[str, Any],
        expected: Mapping[str, Any],
        *,
        idempotency_key: str = "",
    ) -> None:
        """A compare-and-swap update: set ``changes`` on the rows matching ``where``,
        only if ``expected`` still equals the current row. An empty ``expected`` is
        refused, so a guarded update never degrades into an unconditional one."""
        self._check_expected("update_guarded", expected)
        self._table._guarded_update(where, changes, expected, idempotency_key)

    def delete_guarded(
        self, where: Filter, expected: Mapping[str, Any], *, idempotency_key: str = ""
    ) -> None:
        """Delete the matched row only if ``expected`` still equals it. An empty
        ``expected`` is refused before the RPC."""
        self._check_expected("delete_guarded", expected)
        self._table._guarded_delete(where, expected, idempotency_key)


class AsyncRepo(_RepoBase[Row]):
    """:class:`Repo` for asyncio. ``db`` is a ``UdbAsyncProject`` or ``UdbAsyncClient``."""

    def __init__(self, db: Any, spec: TableSpec[Row]):
        self._table: AsyncTable[Row] = AsyncTable(db, spec)
        self._spec = spec

    async def list(self, where: Filter | None = None, **options: Any) -> TablePage[Row]:
        """See :meth:`Repo.list`."""
        return await self._table.select(where, **options)

    async def get(self, where: Filter, *, include_pii: bool = False) -> Row | None:
        """See :meth:`Repo.get`."""
        self._check_full_key(where)
        page = await self._table.select(where, limit=2, include_pii=include_pii)
        return self._at_most_one(page.rows)

    async def find_first(self, where: Filter, *, include_pii: bool = False) -> Row | None:
        """See :meth:`Repo.find_first`."""
        rows = (await self._table.select(where, limit=1, include_pii=include_pii)).rows
        return rows[0] if rows else None

    async def update_guarded(
        self,
        where: Filter,
        changes: Mapping[str, Any],
        expected: Mapping[str, Any],
        *,
        idempotency_key: str = "",
    ) -> None:
        """See :meth:`Repo.update_guarded`."""
        self._check_expected("update_guarded", expected)
        await self._table._guarded_update(where, changes, expected, idempotency_key)

    async def delete_guarded(
        self, where: Filter, expected: Mapping[str, Any], *, idempotency_key: str = ""
    ) -> None:
        """See :meth:`Repo.delete_guarded`."""
        self._check_expected("delete_guarded", expected)
        await self._table._guarded_delete(where, expected, idempotency_key)


def _empty_json_text_is_unset(field: Any, column: Any) -> bool:
    """A nullable JSON text column with no field presence: proto cannot tell "" from
    unset, so a row leaves "" out. A patch names its columns and still sends it."""
    return (
        field.type == FieldDescriptor.TYPE_STRING
        and _is_json(column)
        and not is_repeated(field)
        and not column.not_null
        and not field.has_presence
    )


def table_spec_of(message: Any) -> TableSpec[dict[str, Any]]:
    """The :class:`TableSpec` of an entity message class, read from its annotations.

    The result behaves like the generator's: rows are dicts keyed by proto field name,
    enums in text columns read as full value names and write as stored tokens, and a
    NULL column is absent from a row. Use it for a core entity with no generated
    module, e.g. ``Table(project, table_spec_of(File))``.
    """
    descriptor = message.DESCRIPTOR
    fields: list[str] = []
    enums: dict[str, str] = {}
    big_ints: list[str] = []
    no_insert: list[str] = []
    empty_unset: list[str] = []
    json_text: list[str] = []
    excluded = [name for name in (tenant_column(descriptor), project_column(descriptor)) if name]
    for field_descriptor in descriptor.fields:
        column = column_options(field_descriptor)
        if column is None:
            continue
        name = field_descriptor.name
        fields.append(name)
        if column.generated or column.exclude_from_insert:
            no_insert.append(name)
        if _empty_json_text_is_unset(field_descriptor, column):
            empty_unset.append(name)
        if (
            field_descriptor.type == FieldDescriptor.TYPE_STRING
            and _is_json(column)
            and not is_repeated(field_descriptor)
        ):
            json_text.append(name)
        if column.generated or column.exclude_from_update or column.tenant_column:
            excluded.append(name)
        if field_descriptor.type == FieldDescriptor.TYPE_ENUM and _text_column(column):
            enums[name] = enum_common_prefix(field_descriptor.enum_type)
        if field_descriptor.type in _INT64_TYPES:
            big_ints.append(name)
    names = tuple(fields)

    def to_fields(row: Mapping[str, Any]) -> dict[str, Any]:
        return udb_record_fields(row, names, enums)

    def to_record(row: Mapping[str, Any]) -> dict[str, Any]:
        record = to_fields(row)
        for name in no_insert:
            record.pop(name, None)
        for name in empty_unset:
            if record.get(name) == "":
                del record[name]
        return record

    def from_row(record: Mapping[str, Any]) -> dict[str, Any]:
        row = udb_row_from_record(record, names, enums, big_ints)
        for name in json_text:
            if isinstance(row.get(name), str):
                row[name] = json_column_text(row[name])
        return row

    return TableSpec(
        message_type=descriptor.full_name,
        key=tuple(primary_keys(descriptor)),
        fields=names,
        to_record=to_record,
        to_fields=to_fields,
        from_row=from_row,
        update_excluded=tuple(excluded),
    )


class Column(Generic[Row]):
    """A typed predicate on one column. Each method returns a filter."""

    def __init__(self, field_name: str, enum_prefix: str = ""):
        self.field = field_name
        self._enum_prefix = enum_prefix

    def _value(self, value: Any) -> Any:
        if value is None:
            raise ValueError(f"udb: predicate {self.field}: value is NULL or omitted; use is_null")
        if isinstance(value, int) and not isinstance(value, bool) and abs(value) > _EXACT_FILTER_NUMBER:
            # A Struct number is a double, so the exact digits travel as text.
            return str(value)
        return udb_enum_to_text(value, self._enum_prefix) if self._enum_prefix else value

    def _compare(self, op: str, value: Any) -> dict[str, Any]:
        return {self.field: {op: self._value(value)}}

    def eq(self, value: Any) -> dict[str, Any]:
        return self._compare("$eq", value)

    def gt(self, value: Any) -> dict[str, Any]:
        return self._compare("$gt", value)

    def gte(self, value: Any) -> dict[str, Any]:
        return self._compare("$gte", value)

    def lt(self, value: Any) -> dict[str, Any]:
        return self._compare("$lt", value)

    def lte(self, value: Any) -> dict[str, Any]:
        return self._compare("$lte", value)

    def in_(self, *values: Any) -> dict[str, Any]:
        return {self.field: {"$in": [self._value(value) for value in values]}}

    def not_in(self, *values: Any) -> dict[str, Any]:
        return {self.field: {"$nin": [self._value(value) for value in values]}}

    def between(self, low: Any, high: Any) -> dict[str, Any]:
        return {self.field: {"$between": [self._value(low), self._value(high)]}}

    def is_null(self) -> dict[str, Any]:
        return {self.field: {"$is_null": True}}

    def not_null(self) -> dict[str, Any]:
        return {self.field: {"$not_null": True}}


def udb_enum_to_text(value: Any, prefix: str) -> Any:
    """Stored text of an enum value: the value name without the common prefix."""
    return value[len(prefix) :] if isinstance(value, str) and value.startswith(prefix) else value


def udb_enum_from_text(value: Any, prefix: str) -> Any:
    """The enum value name for stored text."""
    return prefix + value if isinstance(value, str) and not value.startswith(prefix) else value


def udb_big_int(value: Any) -> Any:
    """A 64-bit integer as read: always an exact ``int``."""
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return value
    return value


def udb_record_fields(
    row: Mapping[str, Any], fields: Sequence[str], enums: Mapping[str, str]
) -> dict[str, Any]:
    """The known columns of ``row`` with enum values as stored text. An absent key
    is omitted and ``None`` is kept, so it writes SQL NULL."""
    out: dict[str, Any] = {}
    for name in fields:
        if name in row:
            value = row[name]
            out[name] = udb_enum_to_text(value, enums[name]) if name in enums else value
    return out


def udb_row_from_record(
    record: Mapping[str, Any],
    fields: Sequence[str],
    enums: Mapping[str, str],
    big_ints: Sequence[str],
    masked: Sequence[str] = (),
) -> dict[str, Any]:
    """The typed row: a NULL or missing column, or a ``masked`` column read as the redaction
    placeholder, is left out."""
    out: dict[str, Any] = {}
    for name in fields:
        value = record.get(name)
        if value is None or (name in masked and value == "***MASKED***"):
            continue
        if name in enums:
            value = udb_enum_from_text(value, enums[name])
        elif name in big_ints:
            value = udb_big_int(value)
        out[name] = value
    return out
