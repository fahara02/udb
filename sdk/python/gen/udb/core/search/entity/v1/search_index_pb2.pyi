import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SearchIndex(_message.Message):
    __slots__ = ("index_id", "tenant_id", "index_name", "source_message_type", "backend", "resource_name", "vector_dims", "tenant_column", "source_cdc_topic", "status", "created_at", "updated_at", "metadata_json")
    INDEX_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_DIMS_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CDC_TOPIC_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    index_id: str
    tenant_id: str
    index_name: str
    source_message_type: str
    backend: str
    resource_name: str
    vector_dims: int
    tenant_column: str
    source_cdc_topic: str
    status: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    metadata_json: str
    def __init__(self, index_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., index_name: _Optional[str] = ..., source_message_type: _Optional[str] = ..., backend: _Optional[str] = ..., resource_name: _Optional[str] = ..., vector_dims: _Optional[int] = ..., tenant_column: _Optional[str] = ..., source_cdc_topic: _Optional[str] = ..., status: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., metadata_json: _Optional[str] = ...) -> None: ...
