import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EmbeddingWorkItem(_message.Message):
    __slots__ = ("work_item_id", "tenant_id", "project_id", "job_id", "source_name", "parent_pk", "point_id", "document_id", "doc_version", "chunk_seq", "chunk_count", "chunk_hash", "chunk_text", "model_id", "target_collection", "status", "attempt_count", "max_attempts", "last_error", "retryable", "token_count", "next_attempt_at", "created_at", "last_emitted_at", "acked_at", "updated_at", "parent_text", "char_start", "char_end", "token_start", "token_end")
    WORK_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_PK_FIELD_NUMBER: _ClassVar[int]
    POINT_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    CHUNK_SEQ_FIELD_NUMBER: _ClassVar[int]
    CHUNK_COUNT_FIELD_NUMBER: _ClassVar[int]
    CHUNK_HASH_FIELD_NUMBER: _ClassVar[int]
    CHUNK_TEXT_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    TOKEN_COUNT_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_EMITTED_AT_FIELD_NUMBER: _ClassVar[int]
    ACKED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    PARENT_TEXT_FIELD_NUMBER: _ClassVar[int]
    CHAR_START_FIELD_NUMBER: _ClassVar[int]
    CHAR_END_FIELD_NUMBER: _ClassVar[int]
    TOKEN_START_FIELD_NUMBER: _ClassVar[int]
    TOKEN_END_FIELD_NUMBER: _ClassVar[int]
    work_item_id: str
    tenant_id: str
    project_id: str
    job_id: str
    source_name: str
    parent_pk: str
    point_id: str
    document_id: str
    doc_version: str
    chunk_seq: int
    chunk_count: int
    chunk_hash: str
    chunk_text: str
    model_id: str
    target_collection: str
    status: str
    attempt_count: int
    max_attempts: int
    last_error: str
    retryable: bool
    token_count: int
    next_attempt_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    last_emitted_at: _timestamp_pb2.Timestamp
    acked_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    parent_text: str
    char_start: int
    char_end: int
    token_start: int
    token_end: int
    def __init__(self, work_item_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., job_id: _Optional[str] = ..., source_name: _Optional[str] = ..., parent_pk: _Optional[str] = ..., point_id: _Optional[str] = ..., document_id: _Optional[str] = ..., doc_version: _Optional[str] = ..., chunk_seq: _Optional[int] = ..., chunk_count: _Optional[int] = ..., chunk_hash: _Optional[str] = ..., chunk_text: _Optional[str] = ..., model_id: _Optional[str] = ..., target_collection: _Optional[str] = ..., status: _Optional[str] = ..., attempt_count: _Optional[int] = ..., max_attempts: _Optional[int] = ..., last_error: _Optional[str] = ..., retryable: bool = ..., token_count: _Optional[int] = ..., next_attempt_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_emitted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., acked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., parent_text: _Optional[str] = ..., char_start: _Optional[int] = ..., char_end: _Optional[int] = ..., token_start: _Optional[int] = ..., token_end: _Optional[int] = ...) -> None: ...
