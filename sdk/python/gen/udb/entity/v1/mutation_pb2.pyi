from udb.entity.v1 import operation_pb2 as _operation_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MutationResponse(_message.Message):
    __slots__ = ("mutation_id", "resource_uri", "checksum_sha256", "record_json", "affected_rows", "was_duplicate", "write_receipt_json", "resource_version", "metadata", "warnings")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    MUTATION_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_SHA256_FIELD_NUMBER: _ClassVar[int]
    RECORD_JSON_FIELD_NUMBER: _ClassVar[int]
    AFFECTED_ROWS_FIELD_NUMBER: _ClassVar[int]
    WAS_DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    WRITE_RECEIPT_JSON_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    mutation_id: str
    resource_uri: str
    checksum_sha256: str
    record_json: bytes
    affected_rows: int
    was_duplicate: bool
    write_receipt_json: str
    resource_version: str
    metadata: _containers.ScalarMap[str, str]
    warnings: _containers.RepeatedCompositeFieldContainer[_operation_pb2.OperationWarning]
    def __init__(self, mutation_id: _Optional[str] = ..., resource_uri: _Optional[str] = ..., checksum_sha256: _Optional[str] = ..., record_json: _Optional[bytes] = ..., affected_rows: _Optional[int] = ..., was_duplicate: bool = ..., write_receipt_json: _Optional[str] = ..., resource_version: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., warnings: _Optional[_Iterable[_Union[_operation_pb2.OperationWarning, _Mapping]]] = ...) -> None: ...
