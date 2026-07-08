from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.scheduler.entity.v1 import scheduled_job_pb2 as _scheduled_job_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateJobRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "name", "schedule_type", "cron_expression", "next_fire_at", "payload", "target_topic", "max_attempts", "backoff_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_TYPE_FIELD_NUMBER: _ClassVar[int]
    CRON_EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIRE_AT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TARGET_TOPIC_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    name: str
    schedule_type: str
    cron_expression: str
    next_fire_at: str
    payload: str
    target_topic: str
    max_attempts: int
    backoff_seconds: int
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., name: _Optional[str] = ..., schedule_type: _Optional[str] = ..., cron_expression: _Optional[str] = ..., next_fire_at: _Optional[str] = ..., payload: _Optional[str] = ..., target_topic: _Optional[str] = ..., max_attempts: _Optional[int] = ..., backoff_seconds: _Optional[int] = ...) -> None: ...

class CreateJobResponse(_message.Message):
    __slots__ = ("job_id", "message", "error")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, job_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetJobRequest(_message.Message):
    __slots__ = ("tenant_id", "job_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    job_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., job_id: _Optional[str] = ...) -> None: ...

class GetJobResponse(_message.Message):
    __slots__ = ("job", "error")
    JOB_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    job: _scheduled_job_pb2.ScheduledJob
    error: _dto_pb2.ApiError
    def __init__(self, job: _Optional[_Union[_scheduled_job_pb2.ScheduledJob, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListJobsRequest(_message.Message):
    __slots__ = ("tenant_id", "status", "page", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    status: str
    page: int
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., status: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListJobsResponse(_message.Message):
    __slots__ = ("jobs", "total_count", "error", "next_page_token")
    JOBS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[_scheduled_job_pb2.ScheduledJob]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, jobs: _Optional[_Iterable[_Union[_scheduled_job_pb2.ScheduledJob, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class DeleteJobRequest(_message.Message):
    __slots__ = ("tenant_id", "job_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    job_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., job_id: _Optional[str] = ...) -> None: ...

class DeleteJobResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class PauseJobRequest(_message.Message):
    __slots__ = ("tenant_id", "job_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    job_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., job_id: _Optional[str] = ...) -> None: ...

class PauseJobResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ResumeJobRequest(_message.Message):
    __slots__ = ("tenant_id", "job_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    job_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., job_id: _Optional[str] = ...) -> None: ...

class ResumeJobResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
