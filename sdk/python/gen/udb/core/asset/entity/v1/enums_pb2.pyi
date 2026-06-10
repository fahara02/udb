from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class PipelineStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PIPELINE_STATUS_UNSPECIFIED: _ClassVar[PipelineStatus]
    PIPELINE_STATUS_PENDING: _ClassVar[PipelineStatus]
    PIPELINE_STATUS_RUNNING: _ClassVar[PipelineStatus]
    PIPELINE_STATUS_COMPLETED: _ClassVar[PipelineStatus]
    PIPELINE_STATUS_FAILED: _ClassVar[PipelineStatus]

class StepStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STEP_STATUS_UNSPECIFIED: _ClassVar[StepStatus]
    STEP_STATUS_PENDING: _ClassVar[StepStatus]
    STEP_STATUS_RUNNING: _ClassVar[StepStatus]
    STEP_STATUS_COMPLETED: _ClassVar[StepStatus]
    STEP_STATUS_SKIPPED: _ClassVar[StepStatus]
    STEP_STATUS_FAILED: _ClassVar[StepStatus]

class StepType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STEP_TYPE_UNSPECIFIED: _ClassVar[StepType]
    STEP_TYPE_EMBED: _ClassVar[StepType]
    STEP_TYPE_THUMBNAIL: _ClassVar[StepType]
    STEP_TYPE_RESIZE: _ClassVar[StepType]
    STEP_TYPE_TRANSCODE: _ClassVar[StepType]
    STEP_TYPE_CAPTION: _ClassVar[StepType]
    STEP_TYPE_EXTRACT: _ClassVar[StepType]

class AssetStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ASSET_STATUS_UNSPECIFIED: _ClassVar[AssetStatus]
    ASSET_STATUS_PENDING: _ClassVar[AssetStatus]
    ASSET_STATUS_READY: _ClassVar[AssetStatus]
    ASSET_STATUS_FAILED: _ClassVar[AssetStatus]
PIPELINE_STATUS_UNSPECIFIED: PipelineStatus
PIPELINE_STATUS_PENDING: PipelineStatus
PIPELINE_STATUS_RUNNING: PipelineStatus
PIPELINE_STATUS_COMPLETED: PipelineStatus
PIPELINE_STATUS_FAILED: PipelineStatus
STEP_STATUS_UNSPECIFIED: StepStatus
STEP_STATUS_PENDING: StepStatus
STEP_STATUS_RUNNING: StepStatus
STEP_STATUS_COMPLETED: StepStatus
STEP_STATUS_SKIPPED: StepStatus
STEP_STATUS_FAILED: StepStatus
STEP_TYPE_UNSPECIFIED: StepType
STEP_TYPE_EMBED: StepType
STEP_TYPE_THUMBNAIL: StepType
STEP_TYPE_RESIZE: StepType
STEP_TYPE_TRANSCODE: StepType
STEP_TYPE_CAPTION: StepType
STEP_TYPE_EXTRACT: StepType
ASSET_STATUS_UNSPECIFIED: AssetStatus
ASSET_STATUS_PENDING: AssetStatus
ASSET_STATUS_READY: AssetStatus
ASSET_STATUS_FAILED: AssetStatus
