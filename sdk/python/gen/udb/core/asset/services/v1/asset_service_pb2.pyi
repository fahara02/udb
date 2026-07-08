from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.asset.entity.v1 import asset_pb2 as _asset_pb2
from udb.core.asset.entity.v1 import pipeline_definition_pb2 as _pipeline_definition_pb2
from udb.core.asset.entity.v1 import pipeline_instance_pb2 as _pipeline_instance_pb2
from udb.core.asset.entity.v1 import pipeline_step_pb2 as _pipeline_step_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreatePipelineDefinitionRequest(_message.Message):
    __slots__ = ("tenant_id", "name", "description", "media_type", "steps", "version")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    name: str
    description: str
    media_type: str
    steps: str
    version: int
    def __init__(self, tenant_id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., media_type: _Optional[str] = ..., steps: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class CreatePipelineDefinitionResponse(_message.Message):
    __slots__ = ("definition_id", "message", "error")
    DEFINITION_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    definition_id: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, definition_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetPipelineDefinitionRequest(_message.Message):
    __slots__ = ("tenant_id", "definition_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    definition_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., definition_id: _Optional[str] = ...) -> None: ...

class GetPipelineDefinitionResponse(_message.Message):
    __slots__ = ("definition", "error")
    DEFINITION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    definition: _pipeline_definition_pb2.PipelineDefinition
    error: _dto_pb2.ApiError
    def __init__(self, definition: _Optional[_Union[_pipeline_definition_pb2.PipelineDefinition, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RegisterAssetRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "file_id", "name", "media_type", "metadata")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    file_id: str
    name: str
    media_type: str
    metadata: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., file_id: _Optional[str] = ..., name: _Optional[str] = ..., media_type: _Optional[str] = ..., metadata: _Optional[str] = ...) -> None: ...

class RegisterAssetResponse(_message.Message):
    __slots__ = ("asset_id", "message", "error")
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    asset_id: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, asset_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class StartPipelineRequest(_message.Message):
    __slots__ = ("tenant_id", "definition_id", "asset_id", "context", "correlation_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    definition_id: str
    asset_id: str
    context: str
    correlation_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., definition_id: _Optional[str] = ..., asset_id: _Optional[str] = ..., context: _Optional[str] = ..., correlation_id: _Optional[str] = ...) -> None: ...

class StartPipelineResponse(_message.Message):
    __slots__ = ("instance_id", "message", "error", "steps")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    message: str
    error: _dto_pb2.ApiError
    steps: _containers.RepeatedCompositeFieldContainer[_pipeline_step_pb2.PipelineStep]
    def __init__(self, instance_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., steps: _Optional[_Iterable[_Union[_pipeline_step_pb2.PipelineStep, _Mapping]]] = ...) -> None: ...

class GetPipelineRequest(_message.Message):
    __slots__ = ("tenant_id", "instance_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    instance_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., instance_id: _Optional[str] = ...) -> None: ...

class GetPipelineResponse(_message.Message):
    __slots__ = ("instance", "steps", "error")
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    instance: _pipeline_instance_pb2.PipelineInstance
    steps: _containers.RepeatedCompositeFieldContainer[_pipeline_step_pb2.PipelineStep]
    error: _dto_pb2.ApiError
    def __init__(self, instance: _Optional[_Union[_pipeline_instance_pb2.PipelineInstance, _Mapping]] = ..., steps: _Optional[_Iterable[_Union[_pipeline_step_pb2.PipelineStep, _Mapping]]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class CompleteStepRequest(_message.Message):
    __slots__ = ("tenant_id", "step_id", "status", "result", "error_message")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    step_id: str
    status: str
    result: str
    error_message: str
    def __init__(self, tenant_id: _Optional[str] = ..., step_id: _Optional[str] = ..., status: _Optional[str] = ..., result: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class CompleteStepResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListAssetsRequest(_message.Message):
    __slots__ = ("tenant_id", "media_type", "status", "page", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    media_type: str
    status: str
    page: int
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., media_type: _Optional[str] = ..., status: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListAssetsResponse(_message.Message):
    __slots__ = ("assets", "total_count", "error", "next_page_token")
    ASSETS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    assets: _containers.RepeatedCompositeFieldContainer[_asset_pb2.Asset]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, assets: _Optional[_Iterable[_Union[_asset_pb2.Asset, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetAssetRequest(_message.Message):
    __slots__ = ("tenant_id", "asset_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    asset_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., asset_id: _Optional[str] = ...) -> None: ...

class GetAssetResponse(_message.Message):
    __slots__ = ("asset", "error")
    ASSET_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    asset: _asset_pb2.Asset
    error: _dto_pb2.ApiError
    def __init__(self, asset: _Optional[_Union[_asset_pb2.Asset, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
