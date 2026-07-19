import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EmbeddingJob(_message.Message):
    __slots__ = ("job_id", "tenant_id", "project_id", "source_name", "document_id", "job_type", "mode", "status", "rows_enumerated", "chunks_emitted", "vectors_stored", "failed", "error", "metadata_json", "created_at", "started_at", "finished_at", "updated_at")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_TYPE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ROWS_ENUMERATED_FIELD_NUMBER: _ClassVar[int]
    CHUNKS_EMITTED_FIELD_NUMBER: _ClassVar[int]
    VECTORS_STORED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    tenant_id: str
    project_id: str
    source_name: str
    document_id: str
    job_type: str
    mode: str
    status: str
    rows_enumerated: int
    chunks_emitted: int
    vectors_stored: int
    failed: int
    error: str
    metadata_json: str
    created_at: _timestamp_pb2.Timestamp
    started_at: _timestamp_pb2.Timestamp
    finished_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, job_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., source_name: _Optional[str] = ..., document_id: _Optional[str] = ..., job_type: _Optional[str] = ..., mode: _Optional[str] = ..., status: _Optional[str] = ..., rows_enumerated: _Optional[int] = ..., chunks_emitted: _Optional[int] = ..., vectors_stored: _Optional[int] = ..., failed: _Optional[int] = ..., error: _Optional[str] = ..., metadata_json: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., finished_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
