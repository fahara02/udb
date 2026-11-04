"""How live-query rows and vector payloads become what the caller asked for."""

from __future__ import annotations

from typing import Any, Mapping

from .exceptions import UdbError
from .record_codec import decode_record
from .schema import message_type


def message_class(message: Any) -> Any:
    """The message class to decode rows into; ``None`` for a type name or no message."""
    if message is None or isinstance(message, str):
        return None
    return message if isinstance(message, type) else type(message)


def message_type_of(message: Any) -> str:
    """The udb message type of a message class, an instance, or a full type name."""
    return message if isinstance(message, str) else message_type(message)


def decode_row(record: Mapping[str, Any], message: Any) -> Any:
    """``record`` decoded as ``message``, or ``record`` itself without a message class."""
    if message is None:
        return record
    try:
        return decode_record(record, message())
    except ValueError as error:
        raise UdbError(str(error)) from error
