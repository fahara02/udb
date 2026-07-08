from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class ScheduleType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SCHEDULE_TYPE_UNSPECIFIED: _ClassVar[ScheduleType]
    SCHEDULE_TYPE_CRON: _ClassVar[ScheduleType]
    SCHEDULE_TYPE_ONE_SHOT: _ClassVar[ScheduleType]

class JobStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    JOB_STATUS_UNSPECIFIED: _ClassVar[JobStatus]
    JOB_STATUS_ACTIVE: _ClassVar[JobStatus]
    JOB_STATUS_PAUSED: _ClassVar[JobStatus]
    JOB_STATUS_COMPLETED: _ClassVar[JobStatus]
    JOB_STATUS_DEAD: _ClassVar[JobStatus]
SCHEDULE_TYPE_UNSPECIFIED: ScheduleType
SCHEDULE_TYPE_CRON: ScheduleType
SCHEDULE_TYPE_ONE_SHOT: ScheduleType
JOB_STATUS_UNSPECIFIED: JobStatus
JOB_STATUS_ACTIVE: JobStatus
JOB_STATUS_PAUSED: JobStatus
JOB_STATUS_COMPLETED: JobStatus
JOB_STATUS_DEAD: JobStatus
