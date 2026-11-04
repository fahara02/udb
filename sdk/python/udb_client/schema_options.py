"""Schema reflection a service needs beyond :mod:`udb_client.schema`: unique lookup
keys, column bounds and defaults, owner columns, retention, and the graph label.

Every function accepts a message instance or a message class and reads the udb
annotations on its generated descriptor, so no service restates the schema.
"""

from __future__ import annotations

import re
from typing import Any

from google.protobuf.descriptor import FieldDescriptor

from udb.core.common.v1 import db_pb2

from .exceptions import UdbConfigurationError
from .schema import (
    _descriptor,
    _extension,
    column_options,
    column_security_options,
    event_type,
    is_repeated,
    message_type,
    partition_key_field,
    primary_key,
    primary_keys,
    project_column,
    table_options,
    table_security_options,
    tenant_column,
    topic,
)

__all__ = [
    "apply_column_defaults",
    "event_type",
    "export_eligible",
    "graph_label",
    "max_len",
    "message_type",
    "owner_column",
    "owner_columns",
    "partition_key_field",
    "primary_key",
    "primary_keys",
    "project_column",
    "retention_class",
    "soft_delete",
    "tenant_column",
    "topic",
    "unique_keys",
]

_INT32_TYPES = (FieldDescriptor.TYPE_INT32, FieldDescriptor.TYPE_SINT32, FieldDescriptor.TYPE_SFIXED32)
_INT64_TYPES = (FieldDescriptor.TYPE_INT64, FieldDescriptor.TYPE_SINT64, FieldDescriptor.TYPE_SFIXED64)
_BOOL_TOKENS = {
    "1": True, "t": True, "T": True, "TRUE": True, "true": True, "True": True,
    "0": False, "f": False, "F": False, "FALSE": False, "false": False, "False": False,
}  # fmt: skip
_INTEGER = re.compile(r"[+-]?[0-9]+")
_PLAIN_INDEX_COLUMN = re.compile(r"[_a-zA-Z][_a-zA-Z0-9]*")


def _sql_name(raw: str) -> str:
    """The broker's snake_case rule for column names: acronym boundaries,
    separators and repeated underscores, so descriptor aliases compare equal."""
    chars = raw.strip()
    out: list[str] = []
    for index, char in enumerate(chars):
        if char.isupper():
            following = chars[index + 1] if index + 1 < len(chars) else ""
            if index > 0 and (not chars[index - 1].isupper() or following.islower()):
                out.append("_")
            out.append(char.lower())
        elif char in "- ":
            out.append("_")
        else:
            out.append(char)
    return re.sub(r"_+", "_", "".join(out)).strip("_")


def _column_sql_name(field: FieldDescriptor) -> str:
    column = column_options(field)
    return _sql_name((column.column_name if column is not None else "") or field.name)


def _split_index_columns(raw: str) -> list[str]:
    # Commas inside an expression or a quoted string do not split a column entry.
    names: list[str] = []
    depth = start = position = 0
    quoted = False
    while position < len(raw):
        char = raw[position]
        if char == "'":
            if quoted and raw[position + 1 : position + 2] == "'":
                position += 2
                continue
            quoted = not quoted
        elif char == "(" and not quoted:
            depth += 1
        elif char == ")" and not quoted:
            depth -= 1
        elif char == "," and not quoted and depth == 0:
            name = raw[start:position].strip()
            if name:
                names.append(name)
            start = position + 1
        position += 1
    name = raw[start:].strip()
    if name:
        names.append(name)
    return names


def unique_keys(message: Any) -> list[list[str]]:
    """Unconditional unique lookup keys, as protobuf field names.

    Composite order is preserved. Primary keys, duplicates, partial indexes and
    expression indexes are excluded. Partition columns are included because the
    generated SQL enforces uniqueness within a partition. An ambiguous or unknown
    ordinary column raises :class:`UdbConfigurationError` rather than advertise an
    unsafe lookup. Lookup components must be non-NULL: SQL unique constraints
    permit NULL rows.
    """
    if message is None:
        raise UdbConfigurationError("udb: unique_keys: no message")
    descriptor = _descriptor(message)
    name_of = message_type(message)

    def resolve(raw: str) -> str:
        wanted = raw.strip().casefold()
        matches = [
            field.name
            for field in descriptor.fields
            if column_options(field) is not None
            and wanted in (field.name.casefold(), _column_sql_name(field).casefold())
        ]
        if len(matches) != 1:
            raise UdbConfigurationError(
                f"udb: {name_of} unique key column {raw.strip()!r} resolves to {len(matches)} fields"
            )
        return matches[0]

    table = table_options(message)
    partition = ""
    if (
        table is not None
        and table.partition_strategy
        not in (db_pb2.PARTITION_STRATEGY_UNSPECIFIED, db_pb2.PARTITION_STRATEGY_NONE)
        and table.partition_column
    ):
        partition = resolve(table.partition_column)

    def effective(names: list[str]) -> list[str]:
        key: list[str] = []
        for name in names:
            field_name = resolve(name)
            if field_name in key:
                raise UdbConfigurationError(f"udb: {name_of} unique key repeats field {field_name!r}")
            key.append(field_name)
        if partition and partition not in key:
            key.append(partition)
        return key

    def key_set(key: list[str]) -> tuple[str, ...]:
        return tuple(sorted(key))

    declared_primary = primary_keys(message)
    seen = {key_set(effective(declared_primary) if declared_primary else [])}
    found: list[list[str]] = []

    def add(names: list[str]) -> None:
        if not names:
            raise UdbConfigurationError(f"udb: {name_of} unique index has no columns")
        key = effective(names)
        if key_set(key) not in seen:
            found.append(key)

    indexes: list[tuple[Any, str]] = [(index, "") for index in (table.indexes if table is not None else ())]
    for field in descriptor.fields:
        column = column_options(field)
        if column is None:
            continue
        if column.unique and not column.primary_key:
            add([field.name])
        if column.HasField("index"):
            # A field index starts with its owning SQL column and omits explicit
            # repetitions of that same name.
            indexes.append((column.index, _column_sql_name(field)))
    indexes.sort(key=lambda item: item[0].index_name)

    for index, local in indexes:
        if not index.unique or index.where_clause.strip():
            continue
        names = [local] if local else []
        expression = bool(local) and _PLAIN_INDEX_COLUMN.fullmatch(local) is None
        for raw in (*index.columns, *index.composite_fields):
            for name in _split_index_columns(raw):
                if "(" in name or "'" in name:
                    expression = True
                    continue
                name = _sql_name(name)
                if _PLAIN_INDEX_COLUMN.fullmatch(name) is None:
                    expression = True
                if not local or local.casefold() != name.casefold():
                    names.append(name)
        if not expression:
            add(names)

    found.sort(key="\x00".join)
    unique: list[list[str]] = []
    for key in found:
        identity = key_set(key)
        if identity not in seen:
            seen.add(identity)
            unique.append(key)
    return unique


def owner_columns(message: Any) -> list[str]:
    """The columns annotated ``owner_field``; they drive privacy export and erasure."""
    columns: list[str] = []
    for field in _descriptor(message).fields:
        security = column_security_options(field)
        if security is not None and security.owner_field:
            columns.append(field.name)
    return columns


def owner_column(message: Any) -> str:
    """The first ``owner_field`` column (``""`` for a tenant-wide table)."""
    columns = owner_columns(message)
    return columns[0] if columns else ""


def export_eligible(message: Any) -> bool:
    """Whether the table's rows belong in a privacy export."""
    security = table_security_options(message)
    return bool(security is not None and security.export_eligible)


def retention_class(message: Any) -> str:
    """The table's retention class (``""`` when it declares none)."""
    security = table_security_options(message)
    return security.retention_class if security is not None else ""


def soft_delete(message: Any) -> bool:
    """Whether the table is declared ``soft_delete``, so a delete tombstones its rows."""
    table = table_options(message)
    return bool(table is not None and table.soft_delete)


def max_len(message: Any, field: str) -> int:
    """``n`` for a column declared ``VARCHAR(n)``; 0 for an unknown field or no such bound."""
    descriptor = _descriptor(message).fields_by_name.get(field)
    column = column_options(descriptor) if descriptor is not None else None
    if column is None:
        return 0
    sql_type = column.sql_type
    if not sql_type.startswith("VARCHAR(") or not sql_type.endswith(")"):
        return 0
    bound = sql_type[len("VARCHAR(") : -1]
    return int(bound) if _INTEGER.fullmatch(bound) else 0


def apply_column_defaults(message: Any) -> None:
    """Set every scalar bool and integer field whose column declares a ``default_value``.

    A row a service creates then starts from the table's own defaults instead of a
    restated value. A default that does not parse for the field's kind raises
    :class:`UdbConfigurationError`.
    """
    for field in message.DESCRIPTOR.fields:
        column = column_options(field)
        default = column.default_value.strip() if column is not None else ""
        if not default or is_repeated(field):
            continue

        def invalid() -> UdbConfigurationError:
            return UdbConfigurationError(
                f"udb: {message_type(message)}.{field.name} default {default!r} "
                f"does not parse for this field"
            )

        if field.type == FieldDescriptor.TYPE_BOOL:
            if default not in _BOOL_TOKENS:
                raise invalid()
            setattr(message, field.name, _BOOL_TOKENS[default])
        elif field.type in _INT32_TYPES or field.type in _INT64_TYPES:
            if not _INTEGER.fullmatch(default):
                raise invalid()
            value = int(default)
            bits = 31 if field.type in _INT32_TYPES else 63
            if not -(2**bits) <= value < 2**bits:
                raise invalid()
            setattr(message, field.name, value)


def graph_label(message: Any) -> str:
    """The node label UDB projects an entity under: the graph ``data_store``'s
    ``node_label`` (or ``udb.neo4j_label``), else its resource name, else the table name."""
    store = _extension(_descriptor(message).GetOptions(), db_pb2.data_store)
    if store is not None and (store.backend == "neo4j" or store.store_kind == "graph"):
        for key in ("udb.neo4j_label", "node_label"):
            label = store.options.get(key, "")
            if label:
                return label
        if store.resource_name:
            return store.resource_name
    table = table_options(message)
    return table.table_name if table is not None else ""
