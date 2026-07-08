from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.workflow.entity.v1 import workflow_instance_pb2 as _workflow_instance_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StartWorkflowRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "workflow_type", "total_steps", "payload", "compensations", "correlation_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_STEPS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    COMPENSATIONS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    workflow_type: str
    total_steps: int
    payload: str
    compensations: str
    correlation_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., workflow_type: _Optional[str] = ..., total_steps: _Optional[int] = ..., payload: _Optional[str] = ..., compensations: _Optional[str] = ..., correlation_id: _Optional[str] = ...) -> None: ...

class StartWorkflowResponse(_message.Message):
    __slots__ = ("workflow_id", "message", "error")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, workflow_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetWorkflowRequest(_message.Message):
    __slots__ = ("tenant_id", "workflow_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    workflow_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., workflow_id: _Optional[str] = ...) -> None: ...

class GetWorkflowResponse(_message.Message):
    __slots__ = ("workflow", "error")
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    workflow: _workflow_instance_pb2.WorkflowInstance
    error: _dto_pb2.ApiError
    def __init__(self, workflow: _Optional[_Union[_workflow_instance_pb2.WorkflowInstance, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListWorkflowsRequest(_message.Message):
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

class ListWorkflowsResponse(_message.Message):
    __slots__ = ("workflows", "total_count", "error", "next_page_token")
    WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    workflows: _containers.RepeatedCompositeFieldContainer[_workflow_instance_pb2.WorkflowInstance]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, workflows: _Optional[_Iterable[_Union[_workflow_instance_pb2.WorkflowInstance, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CancelWorkflowRequest(_message.Message):
    __slots__ = ("tenant_id", "workflow_id", "reason")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    workflow_id: str
    reason: str
    def __init__(self, tenant_id: _Optional[str] = ..., workflow_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class CancelWorkflowResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class SignalWorkflowRequest(_message.Message):
    __slots__ = ("tenant_id", "workflow_id", "signal_name", "signal_payload")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_NAME_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    workflow_id: str
    signal_name: str
    signal_payload: str
    def __init__(self, tenant_id: _Optional[str] = ..., workflow_id: _Optional[str] = ..., signal_name: _Optional[str] = ..., signal_payload: _Optional[str] = ...) -> None: ...

class SignalWorkflowResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
