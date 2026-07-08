import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EmbeddingSource(_message.Message):
    __slots__ = ("source_id", "tenant_id", "source_name", "source_message_type", "text_fields_json", "target_collection", "model_id", "tenant_column", "source_cdc_topic", "status", "created_at", "updated_at", "metadata_json")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELDS_JSON_FIELD_NUMBER: _ClassVar[int]
    TARGET_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CDC_TOPIC_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    tenant_id: str
    source_name: str
    source_message_type: str
    text_fields_json: str
    target_collection: str
    model_id: str
    tenant_column: str
    source_cdc_topic: str
    status: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    metadata_json: str
    def __init__(self, source_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ..., source_message_type: _Optional[str] = ..., text_fields_json: _Optional[str] = ..., target_collection: _Optional[str] = ..., model_id: _Optional[str] = ..., tenant_column: _Optional[str] = ..., source_cdc_topic: _Optional[str] = ..., status: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., metadata_json: _Optional[str] = ...) -> None: ...
