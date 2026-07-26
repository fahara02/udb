from google.protobuf import struct_pb2 as _struct_pb2
from udb.entity.v1 import context_pb2 as _context_pb2
from udb.entity.v1 import vector_pb2 as _vector_pb2
from udb.entity.v1 import relational_pb2 as _relational_pb2
from udb.entity.v1 import consistency_pb2 as _consistency_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Mutation(_message.Message):
    __slots__ = ("context", "tx_id", "operation", "message_type", "record_json", "payload", "filter", "collection", "vector_points", "commit", "rollback", "bucket", "object_key", "object_data", "content_type", "idempotency_key", "changes", "increments")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TX_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RECORD_JSON_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_POINTS_FIELD_NUMBER: _ClassVar[int]
    COMMIT_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    OBJECT_DATA_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    INCREMENTS_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    tx_id: str
    operation: str
    message_type: str
    record_json: bytes
    payload: _struct_pb2.Struct
    filter: _struct_pb2.Struct
    collection: str
    vector_points: _containers.RepeatedCompositeFieldContainer[_vector_pb2.VectorPointMutation]
    commit: bool
    rollback: bool
    bucket: str
    object_key: str
    object_data: bytes
    content_type: str
    idempotency_key: str
    changes: _struct_pb2.Struct
    increments: _containers.RepeatedCompositeFieldContainer[_relational_pb2.UpdateRequest.Increment]
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., tx_id: _Optional[str] = ..., operation: _Optional[str] = ..., message_type: _Optional[str] = ..., record_json: _Optional[bytes] = ..., payload: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., collection: _Optional[str] = ..., vector_points: _Optional[_Iterable[_Union[_vector_pb2.VectorPointMutation, _Mapping]]] = ..., commit: bool = ..., rollback: bool = ..., bucket: _Optional[str] = ..., object_key: _Optional[str] = ..., object_data: _Optional[bytes] = ..., content_type: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., changes: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., increments: _Optional[_Iterable[_Union[_relational_pb2.UpdateRequest.Increment, _Mapping]]] = ...) -> None: ...

class TxStatus(_message.Message):
    __slots__ = ("state", "tx_id", "mutation_id", "message", "write_receipt")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TX_STATE_UNSPECIFIED: _ClassVar[TxStatus.State]
        TX_STATE_OPEN: _ClassVar[TxStatus.State]
        TX_STATE_COMMITTED: _ClassVar[TxStatus.State]
        TX_STATE_ROLLED_BACK: _ClassVar[TxStatus.State]
        TX_STATE_ERROR: _ClassVar[TxStatus.State]
    TX_STATE_UNSPECIFIED: TxStatus.State
    TX_STATE_OPEN: TxStatus.State
    TX_STATE_COMMITTED: TxStatus.State
    TX_STATE_ROLLED_BACK: TxStatus.State
    TX_STATE_ERROR: TxStatus.State
    STATE_FIELD_NUMBER: _ClassVar[int]
    TX_ID_FIELD_NUMBER: _ClassVar[int]
    MUTATION_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    WRITE_RECEIPT_FIELD_NUMBER: _ClassVar[int]
    state: TxStatus.State
    tx_id: str
    mutation_id: str
    message: str
    write_receipt: _consistency_pb2.WriteReceipt
    def __init__(self, state: _Optional[_Union[TxStatus.State, str]] = ..., tx_id: _Optional[str] = ..., mutation_id: _Optional[str] = ..., message: _Optional[str] = ..., write_receipt: _Optional[_Union[_consistency_pb2.WriteReceipt, _Mapping]] = ...) -> None: ...
