from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class WorkflowStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WORKFLOW_STATUS_UNSPECIFIED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_PENDING: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_RUNNING: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_WAITING_SIGNAL: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_COMPLETED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_COMPENSATING: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_COMPENSATED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_CANCELLED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_FAILED: _ClassVar[WorkflowStatus]
WORKFLOW_STATUS_UNSPECIFIED: WorkflowStatus
WORKFLOW_STATUS_PENDING: WorkflowStatus
WORKFLOW_STATUS_RUNNING: WorkflowStatus
WORKFLOW_STATUS_WAITING_SIGNAL: WorkflowStatus
WORKFLOW_STATUS_COMPLETED: WorkflowStatus
WORKFLOW_STATUS_COMPENSATING: WorkflowStatus
WORKFLOW_STATUS_COMPENSATED: WorkflowStatus
WORKFLOW_STATUS_CANCELLED: WorkflowStatus
WORKFLOW_STATUS_FAILED: WorkflowStatus
