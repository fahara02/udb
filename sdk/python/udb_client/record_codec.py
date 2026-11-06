"""The record codec: the one translation between an entity message and a udb record.

Every column rule comes from the message's udb annotations; nothing here names a
table or a column. :func:`encode_record` and :func:`decode_record` produce the
same records as the Go SDK's ``EncodeRecord`` and ``DecodeRecord`` for the same
messages, so a record written by one SDK reads the same through another.

A JSON column's text becomes the decoded JSON value in the record, so a decimal
inside it with more digits than a float holds is rounded when it is parsed.
"""

from __future__ import annotations

import enum
import json
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from google.protobuf import json_format
from google.protobuf.descriptor import EnumDescriptor, FieldDescriptor
from google.protobuf.message import Message

from .schema import (
    legacy_column_security_options,
    column_options,
    is_map,
    is_masked,
    is_repeated,
    message_type,
    project_column,
    tenant_column,
    topic,
)

Record = dict[str, Any]

_INTEGER_TEXT = re.compile(r"^[+-]?[0-9]+$")
_BOOL_TEXT = {
    "1": True, "t": True, "T": True, "TRUE": True, "true": True, "True": True,
    "0": False, "f": False, "F": False, "FALSE": False, "false": False, "False": False,
}
_INT64_TYPES = frozenset(
    {
        FieldDescriptor.TYPE_INT64,
        FieldDescriptor.TYPE_SINT64,
        FieldDescriptor.TYPE_SFIXED64,
        FieldDescriptor.TYPE_UINT64,
        FieldDescriptor.TYPE_FIXED64,
    }
)
_FLOAT_TYPES = frozenset({FieldDescriptor.TYPE_FLOAT, FieldDescriptor.TYPE_DOUBLE})


class WriteMode(enum.Enum):
    """Which columns a record carries."""

    #: Every insertable column: drops generated and exclude_from_insert columns.
    INSERT = "insert"
    #: The Set of an update: also drops the key, tenant, project and
    #: exclude_from_update columns.
    UPDATE = "update"


WRITE_INSERT = WriteMode.INSERT
WRITE_UPDATE = WriteMode.UPDATE


def _reject_constant(text: str) -> Any:
    raise ValueError(f"{text} is not JSON")


def _loads(text: str) -> Any:
    """Parse JSON strictly: NaN and Infinity are not JSON."""
    return json.loads(text, parse_constant=_reject_constant)


def _go_json(value: Any) -> str:
    """Compact JSON with sorted keys and the HTML escapes Go's encoder applies."""
    text = json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False, allow_nan=False)
    for char, escaped in (
        ("&", "\\u0026"),
        ("<", "\\u003c"),
        (">", "\\u003e"),
        (" ", "\\u2028"),
        (" ", "\\u2029"),
    ):
        text = text.replace(char, escaped)
    return text


def _fill_unset_messages(message: Message, out: dict[str, Any]) -> None:
    """Emit ``None`` for unset singular message fields, at every depth, which is
    the proto JSON form of an unpopulated message."""
    if message.DESCRIPTOR.full_name.startswith("google.protobuf."):
        return
    for field in message.DESCRIPTOR.fields:
        if field.message_type is None or is_map(field):
            continue
        if is_repeated(field):
            values = out.get(field.name)
            if isinstance(values, list):
                for item, encoded in zip(getattr(message, field.name), values):
                    if isinstance(encoded, dict):
                        _fill_unset_messages(item, encoded)
            continue
        if message.HasField(field.name):
            encoded = out.get(field.name)
            if isinstance(encoded, dict):
                _fill_unset_messages(getattr(message, field.name), encoded)
        elif field.containing_oneof is None:
            out[field.name] = None


def _proto_json(message: Message) -> dict[str, Any]:
    out = json_format.MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
        use_integers_for_enums=True,
    )
    _fill_unset_messages(message, out)
    return out


def _carries(column: Any, mode: WriteMode) -> bool:
    if column is None or column.generated:
        return False
    if mode is WriteMode.INSERT:
        return not column.exclude_from_insert
    return not (column.exclude_from_update or column.primary_key or column.tenant_column)


def _sql_type(column: Any) -> str:
    return column.sql_type if column is not None else ""


def _is_json(column: Any) -> bool:
    if column is None:
        return False
    return bool(column.is_json or column.is_jsonb or column.sql_type.upper().startswith("JSON"))


def _no_empty_value(column: Any) -> bool:
    """Whether ``""`` is no value of the column's type."""
    sql_type = _sql_type(column).upper()
    return (
        _is_json(column)
        or sql_type.startswith(("UUID", "CHAR(", "CHARACTER(", "TIMESTAMP"))
        or sql_type == "DATE"
    )


def _text_column(column: Any) -> bool:
    """Whether an enum column stores the value's short token rather than its number."""
    sql_type = _sql_type(column).strip().upper()
    return sql_type.startswith(("VARCHAR", "CHARACTER VARYING", "TEXT", "CHAR", "CITEXT"))


def _enum_common_prefix(descriptor: EnumDescriptor) -> str:
    """The longest common ``UPPER_SNAKE_`` prefix of an enum's value names."""
    names = [value.name for value in descriptor.values]
    if not names:
        return ""
    first = names[0]
    end = len(first)
    for name in names[1:]:
        common = 0
        while common < len(first) and common < len(name) and first[common] == name[common]:
            common += 1
        end = min(end, common)
    cut = first[:end].rfind("_")
    return first[: cut + 1] if cut >= 0 else ""


def enum_common_prefix(descriptor: EnumDescriptor) -> str:
    """The prefix a stored enum token leaves out, e.g. ``FILE_STATUS_``."""
    return _enum_common_prefix(descriptor)


def _enum_tokens(descriptor: EnumDescriptor, value: Any) -> Any:
    prefix = _enum_common_prefix(descriptor)

    def token(number: Any) -> Any:
        if not isinstance(number, int) or isinstance(number, bool):
            return number
        found = descriptor.values_by_number.get(number)
        if found is None:
            return number
        return found.name[len(prefix) :] if found.name.startswith(prefix) else found.name

    if isinstance(value, list):
        return [token(item) for item in value]
    return token(value)


def _exact_ints(value: Any) -> Any:
    if isinstance(value, str):
        return int(value)
    if isinstance(value, list):
        return [_exact_ints(item) for item in value]
    return value


def _has(message: Message, field: FieldDescriptor) -> bool:
    if is_repeated(field):
        return len(getattr(message, field.name)) > 0
    if field.has_presence:
        return message.HasField(field.name)
    return getattr(message, field.name) != field.default_value


_ABSENT = object()


def _encode_column(message: Message, field: FieldDescriptor, column: Any, value: Any) -> Any:
    """The column's value, or ``_ABSENT`` when the record leaves it out."""
    not_null = bool(column is not None and column.not_null)
    if field.has_presence and not is_repeated(field) and not message.HasField(field.name):
        return _ABSENT if not_null else None
    if (
        isinstance(value, str)
        and field.message_type is not None
        and field.message_type.full_name == "google.protobuf.Timestamp"
        and _sql_type(column).strip().upper() == "DATE"
    ):
        # A strict DATE bind takes the date alone.
        return value[:10]
    if field.type in _INT64_TYPES:
        return _exact_ints(value)
    if field.type == FieldDescriptor.TYPE_ENUM and _text_column(column):
        return _enum_tokens(field.enum_type, value)
    if not isinstance(value, str) or is_repeated(field) or field.type != FieldDescriptor.TYPE_STRING:
        return value
    if value == "" and not not_null and not field.has_presence:
        return None
    if value == "" and _no_empty_value(column) and not field.has_presence:
        return _ABSENT
    if _is_json(column) and (value != "" or field.has_presence):
        try:
            return _loads(value)
        except ValueError as error:
            raise ValueError(
                f"udb: encode {message_type(message)}.{field.name}: "
                "JSON column holds text that is not JSON"
            ) from error
    return value


def _is_sensitive(field: FieldDescriptor, column: Any) -> bool:
    legacy = legacy_column_security_options(field)
    if legacy is not None and (legacy.is_pii or legacy.is_encrypted):
        return True
    if is_masked(field):
        return True
    return bool(column is not None and column.encrypted)


def encode_record(message: Message, mode: WriteMode = WriteMode.INSERT) -> Record:
    """Turn an entity message into a udb record keyed by proto field names.

    Fields without a column annotation, generated columns and the columns ``mode``
    excludes are dropped. Enums are numbers, or short tokens in text columns. An
    unset optional field is ``None``, or dropped when its column is NOT NULL. An
    empty string without presence in a nullable column is ``None``; in a NOT NULL
    UUID, CHAR(n), DATE, TIMESTAMP or JSON column it is dropped. A JSON column
    carries its text as a JSON value, and text that is not JSON raises.
    """
    encoded = _proto_json(message)
    tenant = project = ""
    if mode is WriteMode.UPDATE:
        tenant, project = tenant_column(message), project_column(message)
    record: Record = {}
    present = {field.name for field, _ in message.ListFields()}
    for field in message.DESCRIPTOR.fields:
        column = column_options(field)
        # A default read leaves PII and encrypted columns out, so an unset one may only be
        # unread. A full-row write must not erase it: clear it with patch and None.
        if _is_sensitive(field, column) and not is_repeated(field) and field.name not in present:
            continue
        if not _carries(column, mode) or (mode is WriteMode.UPDATE and field.name in (tenant, project)):
            continue
        value = _encode_column(message, field, column, encoded.get(field.name))
        if value is not _ABSENT:
            record[field.name] = value
    return record


def _copy_field(source: Message, target: Message, field: FieldDescriptor) -> None:
    if is_repeated(field):
        if is_map(field):
            getattr(target, field.name).MergeFrom(getattr(source, field.name))
        else:
            getattr(target, field.name).extend(getattr(source, field.name))
    elif field.message_type is not None:
        getattr(target, field.name).CopyFrom(getattr(source, field.name))
    else:
        setattr(target, field.name, getattr(source, field.name))


def _field(message: Message, name: str) -> FieldDescriptor:
    field = message.DESCRIPTOR.fields_by_name.get(name)
    if field is None:
        raise ValueError(f"udb: {message_type(message)} has no field {name!r}")
    return field


def encode_field(message: Message, name: str) -> tuple[Any, bool]:
    """Encode one field exactly as :func:`encode_record` encodes its column.

    Returns ``(value, present)``; ``present`` is False when the column is left out
    of the record. An unknown field raises.
    """
    field = _field(message, name)
    one = type(message)()
    if _has(message, field):
        _copy_field(message, one, field)
    value = _encode_column(one, field, column_options(field), _proto_json(one).get(name))
    if value is _ABSENT:
        return None, False
    return value, True


def decode_field(message: Message, name: str, raw: Any) -> None:
    """Set one field from the value a row holds, accepting every shape
    :func:`decode_record` accepts. ``None`` leaves the field unchanged, and a value
    for a second member of a oneof raises and leaves the message unchanged."""
    field = _field(message, name)
    one = type(message)()
    decode_record({name: raw}, one)
    if not _has(one, field):
        return
    group = field.containing_oneof
    if group is not None:
        previous = message.WhichOneof(group.name)
        if previous is not None and previous != name:
            raise ValueError(
                f"udb: decode {message_type(message)}: oneof {group.name} has both {previous} and {name}"
            )
    message.ClearField(name)
    _copy_field(one, message, field)


def _enum_full_name(descriptor: EnumDescriptor, token: str) -> str:
    if token in descriptor.values_by_name:
        return token
    full = _enum_common_prefix(descriptor) + token
    return full if full in descriptor.values_by_name else token


def _split_array_literal(body: str) -> list[tuple[str, bool]]:
    if not body.strip():
        return []
    out: list[tuple[str, bool]] = []
    current: list[str] = []
    quoted = in_quotes = escaped = False

    def flush() -> None:
        nonlocal quoted
        text = "".join(current)
        out.append((text if quoted else text.strip(), quoted))
        current.clear()
        quoted = False

    for char in body:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == '"':
            in_quotes = not in_quotes
            quoted = True
        elif char == "," and not in_quotes:
            flush()
        elif char == "{" and not in_quotes:
            raise ValueError("nested arrays are not supported")
        else:
            current.append(char)
    if in_quotes or escaped:
        raise ValueError("unterminated element")
    flush()
    return out


def _array_literal(literal: str, field: FieldDescriptor) -> list[Any]:
    """A Postgres array literal such as ``{1,2}`` or ``{a,"b, c"}`` as a list. A NULL
    element raises: a repeated field cannot hold one."""
    literal = literal.strip()
    if not literal.startswith("{") or not literal.endswith("}"):
        raise ValueError(f"not an array literal: {literal!r}")
    out: list[Any] = []
    for text, quoted in _split_array_literal(literal[1:-1]):
        if not quoted and text.upper() == "NULL":
            raise ValueError(f"array literal {literal!r} holds NULL")
        kind = field.type
        if kind in (FieldDescriptor.TYPE_STRING, FieldDescriptor.TYPE_BYTES):
            out.append(text)
        elif kind == FieldDescriptor.TYPE_BOOL:
            out.append(_parse_bool(text))
        elif kind in _FLOAT_TYPES:
            number = float(text)
            if not math.isfinite(number):
                raise ValueError(f"array literal {literal!r} holds a non-finite number")
            out.append(number)
        elif kind == FieldDescriptor.TYPE_ENUM:
            out.append(int(text) if _INTEGER_TEXT.match(text) else text)
        else:
            if not _INTEGER_TEXT.match(text):
                raise ValueError(f"invalid integer {text!r} in array literal")
            out.append(int(text))
    return out


def _parse_bool(text: str) -> bool:
    try:
        return _BOOL_TEXT[text]
    except KeyError:
        raise ValueError(f"invalid boolean {text!r}") from None


def _column_time_text(text: str) -> str:
    """RFC 3339 from the text a TIMESTAMP or DATE column reads as, taking no zone as UTC."""
    trimmed = text.strip()
    if len(trimmed) == 10:
        return f"{trimmed}T00:00:00Z"
    if len(trimmed) > 10 and trimmed[10] == " ":
        trimmed = f"{trimmed[:10]}T{trimmed[11:]}"
    if any(mark in trimmed[10:] for mark in "Zz+-"):
        return trimmed
    return f"{trimmed}Z"


def _normalise(field: FieldDescriptor, value: Any) -> Any:
    """Rewrite one column value into the proto JSON form of its field."""
    if field.message_type is not None:
        if (
            isinstance(value, str)
            and not is_repeated(field)
            and field.message_type.full_name == "google.protobuf.Timestamp"
        ):
            return _column_time_text(value)
        return value
    if is_repeated(field):
        if not isinstance(value, str):
            items: Any = value
        elif value.strip().startswith("["):
            items = _loads(value.strip())
        else:
            items = _array_literal(value, field)
        if field.type == FieldDescriptor.TYPE_ENUM and isinstance(items, list):
            return [
                _enum_full_name(field.enum_type, item) if isinstance(item, str) else item
                for item in items
            ]
        return items
    if field.type == FieldDescriptor.TYPE_STRING:
        if not isinstance(value, str):
            return _go_json(value)
        sql_type = _sql_type(column_options(field)).upper()
        if sql_type.startswith(("CHAR(", "CHARACTER(")):
            return value.rstrip(" ")
        if sql_type == "DATE" and len(value) > 10:
            return value[:10]
        return value
    if not isinstance(value, str):
        return value
    if field.type == FieldDescriptor.TYPE_BOOL:
        return _parse_bool(value.strip())
    if field.type == FieldDescriptor.TYPE_ENUM:
        text = value.strip()
        if _INTEGER_TEXT.match(text):
            return int(text)
        return _enum_full_name(field.enum_type, text)
    return value  # Numbers as text: proto JSON accepts quoted numbers.


def decode_record(record: Mapping[str, Any], message: Message) -> Message:
    """Fill ``message`` from a udb record, replacing what it held, and return it.

    NULL and unknown columns are skipped. A string field whose column came back as a
    JSON value gets that value's JSON text; CHAR(n) padding is trimmed; a DATE with a
    time part keeps its date. Booleans, enums and numbers that came back as text are
    parsed, and an enum takes a short token, a full name or a number. A repeated
    field that came back as text is read as a Postgres array literal or a JSON array.
    An unknown enum name raises rather than decoding as zero.
    """
    clean: dict[str, Any] = {}
    for field in message.DESCRIPTOR.fields:
        value = record.get(field.name)
        if value is None:
            continue
        # A protected column reads as this placeholder. An unprotected text column may
        # really hold the text, so only a protected or non-text column can be sure.
        if value == "***MASKED***" and (
            _is_sensitive(field, column_options(field)) or field.type != FieldDescriptor.TYPE_STRING
        ):
            continue
        try:
            clean[field.name] = _normalise(field, value)
        except ValueError as error:
            raise ValueError(f"udb: decode {message_type(message)}.{field.name}: {error}") from error
    message.Clear()
    try:
        json_format.ParseDict(clean, message)
    except json_format.ParseError as error:
        raise ValueError(f"udb: decode {message_type(message)}: {error}") from error
    return message


def encode_event(event: Message) -> Record:
    """Turn an event message into an outbox payload keyed by proto field names. The
    caller adds the envelope keys. An event that declares no outbox topic raises."""
    if not topic(event):
        raise ValueError(f"udb: {message_type(event)} declares no outbox topic")
    return json_format.MessageToDict(event, preserving_proto_field_name=True)


_RFC3339 = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[Tt](\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?([Zz]|[+-]\d{2}:\d{2})$"
)


def format_timestamp(value: datetime) -> str:
    """Render ``value`` exactly as udb returns a TIMESTAMPTZ: UTC, six fractional
    digits and a Z suffix. A naive datetime is taken as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_timestamp(text: str) -> datetime | None:
    """Parse a TIMESTAMPTZ column (RFC 3339, nanoseconds truncated to microseconds);
    ``""`` (NULL) is ``None``."""
    if text == "":
        return None
    match = _RFC3339.match(text)
    if match is None:
        raise ValueError(f"udb: not an RFC 3339 timestamp: {text!r}")
    date, clock, fraction, zone = match.groups()
    parsed = datetime.strptime(f"{date} {clock}", "%Y-%m-%d %H:%M:%S")
    parsed = parsed.replace(microsecond=int((fraction or "").ljust(6, "0")[:6] or "0"))
    if zone in ("Z", "z"):
        offset = timedelta(0)
    else:
        sign = -1 if zone[0] == "-" else 1
        offset = sign * timedelta(hours=int(zone[1:3]), minutes=int(zone[4:6]))
    return parsed.replace(tzinfo=timezone(offset))

