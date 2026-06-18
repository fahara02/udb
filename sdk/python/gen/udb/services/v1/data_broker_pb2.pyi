from udb.entity.v1 import admin_pb2 as _admin_pb2
from udb.entity.v1 import context_pb2 as _context_pb2
from udb.entity.v1 import blob_pb2 as _blob_pb2
from udb.entity.v1 import cdc_pb2 as _cdc_pb2
from udb.entity.v1 import mutation_pb2 as _mutation_pb2
from udb.entity.v1 import outbox_pb2 as _outbox_pb2
from udb.entity.v1 import record_batch_pb2 as _record_batch_pb2
from udb.entity.v1 import relational_pb2 as _relational_pb2
from udb.entity.v1 import stores_pb2 as _stores_pb2
from udb.entity.v1 import tx_pb2 as _tx_pb2
from udb.entity.v1 import vector_pb2 as _vector_pb2
from udb.events.v1 import udb_events_pb2 as _udb_events_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EnsureBaselineRequest(_message.Message):
    __slots__ = ("context",)
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class EnsureBaselineResponse(_message.Message):
    __slots__ = ("saga_ids", "dlq_ids", "device_id")
    SAGA_IDS_FIELD_NUMBER: _ClassVar[int]
    DLQ_IDS_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    saga_ids: _containers.RepeatedScalarFieldContainer[str]
    dlq_ids: _containers.RepeatedScalarFieldContainer[str]
    device_id: str
    def __init__(self, saga_ids: _Optional[_Iterable[str]] = ..., dlq_ids: _Optional[_Iterable[str]] = ..., device_id: _Optional[str] = ...) -> None: ...
