from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ConsistencyMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONSISTENCY_MODE_UNSPECIFIED: _ClassVar[ConsistencyMode]
    CONSISTENCY_MODE_STRONG: _ClassVar[ConsistencyMode]
    CONSISTENCY_MODE_READ_YOUR_WRITES: _ClassVar[ConsistencyMode]
    CONSISTENCY_MODE_BOUNDED_STALENESS: _ClassVar[ConsistencyMode]
    CONSISTENCY_MODE_REPLICA_BOUNDED: _ClassVar[ConsistencyMode]
    CONSISTENCY_MODE_EVENTUAL: _ClassVar[ConsistencyMode]
    CONSISTENCY_MODE_PROJECTION_OK: _ClassVar[ConsistencyMode]
    CONSISTENCY_MODE_CACHE_OK: _ClassVar[ConsistencyMode]
CONSISTENCY_MODE_UNSPECIFIED: ConsistencyMode
CONSISTENCY_MODE_STRONG: ConsistencyMode
CONSISTENCY_MODE_READ_YOUR_WRITES: ConsistencyMode
CONSISTENCY_MODE_BOUNDED_STALENESS: ConsistencyMode
CONSISTENCY_MODE_REPLICA_BOUNDED: ConsistencyMode
CONSISTENCY_MODE_EVENTUAL: ConsistencyMode
CONSISTENCY_MODE_PROJECTION_OK: ConsistencyMode
CONSISTENCY_MODE_CACHE_OK: ConsistencyMode

class WriteReceipt(_message.Message):
    __slots__ = ("source_lsn", "outbox_seq", "projection_task_ids", "manifest_checksum", "written_at_unix_ms")
    SOURCE_LSN_FIELD_NUMBER: _ClassVar[int]
    OUTBOX_SEQ_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_TASK_IDS_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    WRITTEN_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    source_lsn: str
    outbox_seq: int
    projection_task_ids: _containers.RepeatedScalarFieldContainer[str]
    manifest_checksum: str
    written_at_unix_ms: int
    def __init__(self, source_lsn: _Optional[str] = ..., outbox_seq: _Optional[int] = ..., projection_task_ids: _Optional[_Iterable[str]] = ..., manifest_checksum: _Optional[str] = ..., written_at_unix_ms: _Optional[int] = ...) -> None: ...

class ReadFence(_message.Message):
    __slots__ = ("min_outbox_lsn", "projection_task_ids", "max_wait_ms")
    MIN_OUTBOX_LSN_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_TASK_IDS_FIELD_NUMBER: _ClassVar[int]
    MAX_WAIT_MS_FIELD_NUMBER: _ClassVar[int]
    min_outbox_lsn: str
    projection_task_ids: _containers.RepeatedScalarFieldContainer[str]
    max_wait_ms: int
    def __init__(self, min_outbox_lsn: _Optional[str] = ..., projection_task_ids: _Optional[_Iterable[str]] = ..., max_wait_ms: _Optional[int] = ...) -> None: ...
