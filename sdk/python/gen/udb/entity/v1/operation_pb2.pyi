from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class StoreResource(_message.Message):
    __slots__ = ("backend", "instance", "resource_kind", "resource_name", "resource_uri", "message_type", "schema", "labels")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_KIND_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    backend: str
    instance: str
    resource_kind: str
    resource_name: str
    resource_uri: str
    message_type: str
    schema: str
    labels: _containers.ScalarMap[str, str]
    def __init__(self, backend: _Optional[str] = ..., instance: _Optional[str] = ..., resource_kind: _Optional[str] = ..., resource_name: _Optional[str] = ..., resource_uri: _Optional[str] = ..., message_type: _Optional[str] = ..., schema: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...

class OperationWarning(_message.Message):
    __slots__ = ("code", "message", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class OperationStats(_message.Message):
    __slots__ = ("scanned_count", "matched_count", "affected_count", "returned_count", "elapsed_ms", "backend", "instance")
    SCANNED_COUNT_FIELD_NUMBER: _ClassVar[int]
    MATCHED_COUNT_FIELD_NUMBER: _ClassVar[int]
    AFFECTED_COUNT_FIELD_NUMBER: _ClassVar[int]
    RETURNED_COUNT_FIELD_NUMBER: _ClassVar[int]
    ELAPSED_MS_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    scanned_count: int
    matched_count: int
    affected_count: int
    returned_count: int
    elapsed_ms: int
    backend: str
    instance: str
    def __init__(self, scanned_count: _Optional[int] = ..., matched_count: _Optional[int] = ..., affected_count: _Optional[int] = ..., returned_count: _Optional[int] = ..., elapsed_ms: _Optional[int] = ..., backend: _Optional[str] = ..., instance: _Optional[str] = ...) -> None: ...
