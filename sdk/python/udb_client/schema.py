"""Schema helpers read from the udb annotations on generated proto descriptors.

Everything the SDK needs to know about a table or an event (its message type,
keys, scope columns, event contract) comes from here, so nothing restates the
schema. Backend-neutral options take precedence over the Postgres ones.

Every function accepts a message instance or a message class.
"""

from __future__ import annotations

from typing import Any, Callable

from google.protobuf.descriptor import Descriptor, FieldDescriptor

from udb.core.common.v1 import db_pb2, security_pb2

_TENANT_COLUMNS = ("tenant_id", "_tenant_id", "org_id", "institution_id")
_PROJECT_COLUMNS = ("project_id", "_project_id")


def _descriptor(message: Any) -> Descriptor:
    if isinstance(message, Descriptor):
        return message
    return message.DESCRIPTOR


def is_repeated(field: FieldDescriptor) -> bool:
    """Whether ``field`` is repeated (a map field counts, as it does on the wire)."""
    known = getattr(field, "is_repeated", None)
    if known is not None:
        return bool(known)
    return field.label == FieldDescriptor.LABEL_REPEATED


def is_map(field: FieldDescriptor) -> bool:
    entry = field.message_type
    return bool(entry is not None and is_repeated(field) and entry.GetOptions().map_entry)


def column_options(field: FieldDescriptor) -> Any | None:
    """The field's column annotation, ``None`` when it is not a column."""
    options = field.GetOptions()
    if options.HasExtension(db_pb2.column):
        return options.Extensions[db_pb2.column]
    if options.HasExtension(db_pb2.pg_column):
        return options.Extensions[db_pb2.pg_column]
    return None


def table_options(message: Any) -> Any | None:
    """The message's table annotation, ``None`` when it declares none."""
    options = _descriptor(message).GetOptions()
    if options.HasExtension(db_pb2.table):
        return options.Extensions[db_pb2.table]
    if options.HasExtension(db_pb2.pg_table):
        return options.Extensions[db_pb2.pg_table]
    return None


def _extension(options: Any, extension: Any) -> Any | None:
    return options.Extensions[extension] if options.HasExtension(extension) else None


def column_security_options(field: FieldDescriptor) -> Any | None:
    return _extension(field.GetOptions(), security_pb2.db_column_security)


def legacy_column_security_options(field: FieldDescriptor) -> Any | None:
    return _extension(field.GetOptions(), db_pb2.column_security)


def table_security_options(message: Any) -> Any | None:
    return _extension(_descriptor(message).GetOptions(), security_pb2.db_table_security)


def _flag(field: FieldDescriptor, extension: Any) -> bool:
    options = field.GetOptions()
    return bool(options.HasExtension(extension) and options.Extensions[extension])


def is_masked(field: FieldDescriptor) -> bool:
    """Whether reads mask this column. A secret classification alone does not."""
    legacy = legacy_column_security_options(field)
    security = column_security_options(field)
    view = security_pb2.OutputView.Name(security.output_view) if security is not None else ""
    redaction = (
        security_pb2.RedactionStrategy.Name(security.redaction_strategy)
        if security is not None
        else ""
    )
    return (
        bool(legacy is not None and (legacy.is_pii or legacy.mask_in_logs))
        or _flag(field, security_pb2.pii)
        or _flag(field, security_pb2.log_masked)
        or _flag(field, security_pb2.log_redacted)
        or _flag(field, security_pb2.sensitive)
        or view in ("OUTPUT_VIEW_STORAGE_ONLY", "OUTPUT_VIEW_NEVER")
        or redaction not in ("", "REDACTION_STRATEGY_UNSPECIFIED", "REDACTION_STRATEGY_NONE")
    )


def is_pii(field: FieldDescriptor) -> bool:
    legacy = legacy_column_security_options(field)
    return bool(legacy is not None and legacy.is_pii) or _flag(field, security_pb2.pii)


def is_encrypted(field: FieldDescriptor) -> bool:
    legacy = legacy_column_security_options(field)
    security = column_security_options(field)
    return (
        bool(legacy is not None and legacy.is_encrypted)
        or _flag(field, security_pb2.encrypted_security)
        or bool(security is not None and (security.encryption_key_class or security.searchable_encrypted))
    )


def message_type(message: Any) -> str:
    """The udb message type of an entity message: its proto full name."""
    return _descriptor(message).full_name


def _event_contract(message: Any) -> Any | None:
    return _extension(_descriptor(message).GetOptions(), security_pb2.message_event_contract)


def topic(message: Any) -> str:
    """The outbox topic an event message declares (``""`` when it declares none)."""
    contract = _event_contract(message)
    return contract.outbox_topic if contract is not None else ""


def event_type(message: Any) -> str:
    """The event type an event message's contract declares, else its outbox topic."""
    contract = _event_contract(message)
    declared = contract.event_type if contract is not None else ""
    return declared or topic(message)


def partition_key_field(message: Any) -> str:
    """The payload field an event message names as its partition key."""
    contract = _event_contract(message)
    return contract.partition_key_field if contract is not None else ""


def primary_keys(message: Any) -> list[str]:
    """The primary-key columns declared on an entity message, in field order."""
    keys: list[str] = []
    for field in _descriptor(message).fields:
        column = column_options(field)
        if column is not None and column.primary_key:
            keys.append(field.name)
    return keys


def primary_key(message: Any) -> str:
    """The key column of a single-key table; ``""`` for none or a composite key."""
    keys = primary_keys(message)
    return keys[0] if len(keys) == 1 else ""


def _scope_column(
    message: Any,
    declared: str,
    flagged: Callable[[Any], bool],
    candidates: tuple[str, ...],
) -> str:
    fields = _descriptor(message).fields
    declared = declared.strip()
    if declared:
        for field in fields:
            column = column_options(field)
            if column is None:
                continue
            if field.name == declared or column.column_name.lower() == declared:
                return field.name
    for field in fields:
        column = column_options(field)
        if column is not None and flagged(column):
            return field.name
    for field in fields:
        column = column_options(field)
        if column is None:
            continue
        for name in candidates:
            if field.name.lower() == name or column.column_name.lower() == name:
                return field.name
    return ""


def tenant_column(message: Any) -> str:
    """The field holding the row's tenant: table security, the column flag, then
    the broker's conventional tenant names."""
    security = table_security_options(message)
    return _scope_column(
        message,
        security.tenant_column if security is not None else "",
        lambda column: column.tenant_column,
        _TENANT_COLUMNS,
    )


def project_column(message: Any) -> str:
    """The field holding the row's project, resolved like :func:`tenant_column`."""
    security = table_security_options(message)
    return _scope_column(
        message,
        security.project_column if security is not None else "",
        lambda column: column.project_column,
        _PROJECT_COLUMNS,
    )
