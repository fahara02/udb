import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NotificationSentEvent(_message.Message):
    __slots__ = ("event_id", "log_id", "template_id", "event_type", "channel", "recipient_ref", "tenant_id", "correlation_id", "occurred_at", "project_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LOG_ID_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_REF_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    log_id: str
    template_id: str
    event_type: str
    channel: str
    recipient_ref: str
    tenant_id: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    project_id: str
    def __init__(self, event_id: _Optional[str] = ..., log_id: _Optional[str] = ..., template_id: _Optional[str] = ..., event_type: _Optional[str] = ..., channel: _Optional[str] = ..., recipient_ref: _Optional[str] = ..., tenant_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., project_id: _Optional[str] = ...) -> None: ...

class NotificationFailedEvent(_message.Message):
    __slots__ = ("event_id", "log_id", "template_id", "event_type", "channel", "tenant_id", "error_code", "error_detail", "retry_attempt", "will_retry", "correlation_id", "occurred_at", "project_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LOG_ID_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_DETAIL_FIELD_NUMBER: _ClassVar[int]
    RETRY_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    WILL_RETRY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    log_id: str
    template_id: str
    event_type: str
    channel: str
    tenant_id: str
    error_code: str
    error_detail: str
    retry_attempt: int
    will_retry: bool
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    project_id: str
    def __init__(self, event_id: _Optional[str] = ..., log_id: _Optional[str] = ..., template_id: _Optional[str] = ..., event_type: _Optional[str] = ..., channel: _Optional[str] = ..., tenant_id: _Optional[str] = ..., error_code: _Optional[str] = ..., error_detail: _Optional[str] = ..., retry_attempt: _Optional[int] = ..., will_retry: bool = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., project_id: _Optional[str] = ...) -> None: ...

class NotificationSuppressedEvent(_message.Message):
    __slots__ = ("event_id", "template_id", "event_type", "channel", "recipient_ref", "tenant_id", "suppression_reason", "correlation_id", "occurred_at", "project_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_REF_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUPPRESSION_REASON_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    template_id: str
    event_type: str
    channel: str
    recipient_ref: str
    tenant_id: str
    suppression_reason: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    project_id: str
    def __init__(self, event_id: _Optional[str] = ..., template_id: _Optional[str] = ..., event_type: _Optional[str] = ..., channel: _Optional[str] = ..., recipient_ref: _Optional[str] = ..., tenant_id: _Optional[str] = ..., suppression_reason: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., project_id: _Optional[str] = ...) -> None: ...

class NotificationDeliveredEvent(_message.Message):
    __slots__ = ("event_id", "log_id", "channel", "tenant_id", "correlation_id", "occurred_at", "project_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LOG_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    log_id: str
    channel: str
    tenant_id: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    project_id: str
    def __init__(self, event_id: _Optional[str] = ..., log_id: _Optional[str] = ..., channel: _Optional[str] = ..., tenant_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., project_id: _Optional[str] = ...) -> None: ...

class NotificationTemplateChangedEvent(_message.Message):
    __slots__ = ("event_id", "template_id", "event_type", "channel", "change_type", "changed_by", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    CHANGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANGED_BY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    template_id: str
    event_type: str
    channel: str
    change_type: str
    changed_by: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., template_id: _Optional[str] = ..., event_type: _Optional[str] = ..., channel: _Optional[str] = ..., change_type: _Optional[str] = ..., changed_by: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class ResourceIngestStatusEvent(_message.Message):
    __slots__ = ("event_id", "resource_id", "tenant_id", "project_id", "status", "filename", "mime_type", "size_bytes", "sha256", "error_message", "ingest_job_id", "correlation_id", "occurred_at", "resource_type", "resource_name")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    MIME_TYPE_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    SHA256_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    INGEST_JOB_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    resource_id: str
    tenant_id: str
    project_id: str
    status: str
    filename: str
    mime_type: str
    size_bytes: int
    sha256: str
    error_message: str
    ingest_job_id: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    resource_type: str
    resource_name: str
    def __init__(self, event_id: _Optional[str] = ..., resource_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., status: _Optional[str] = ..., filename: _Optional[str] = ..., mime_type: _Optional[str] = ..., size_bytes: _Optional[int] = ..., sha256: _Optional[str] = ..., error_message: _Optional[str] = ..., ingest_job_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., resource_type: _Optional[str] = ..., resource_name: _Optional[str] = ...) -> None: ...
