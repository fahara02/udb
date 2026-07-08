from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ERROR_KIND_UNSPECIFIED: _ClassVar[ErrorKind]
    ERROR_KIND_CAPABILITY: _ClassVar[ErrorKind]
    ERROR_KIND_POLICY: _ClassVar[ErrorKind]
    ERROR_KIND_QUOTA: _ClassVar[ErrorKind]
    ERROR_KIND_SCHEMA: _ClassVar[ErrorKind]
    ERROR_KIND_RETRYABLE: _ClassVar[ErrorKind]
    ERROR_KIND_INTERNAL: _ClassVar[ErrorKind]
    ERROR_KIND_VALIDATION: _ClassVar[ErrorKind]
ERROR_KIND_UNSPECIFIED: ErrorKind
ERROR_KIND_CAPABILITY: ErrorKind
ERROR_KIND_POLICY: ErrorKind
ERROR_KIND_QUOTA: ErrorKind
ERROR_KIND_SCHEMA: ErrorKind
ERROR_KIND_RETRYABLE: ErrorKind
ERROR_KIND_INTERNAL: ErrorKind
ERROR_KIND_VALIDATION: ErrorKind

class ErrorFieldViolation(_message.Message):
    __slots__ = ("field", "description")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    field: str
    description: str
    def __init__(self, field: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class ErrorDetail(_message.Message):
    __slots__ = ("backend", "operation", "capability_required", "retryable", "retry_after_ms", "policy_decision_id", "correlation_id", "kind", "field_violations")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    CAPABILITY_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_MS_FIELD_NUMBER: _ClassVar[int]
    POLICY_DECISION_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    FIELD_VIOLATIONS_FIELD_NUMBER: _ClassVar[int]
    backend: str
    operation: str
    capability_required: str
    retryable: bool
    retry_after_ms: int
    policy_decision_id: str
    correlation_id: str
    kind: ErrorKind
    field_violations: _containers.RepeatedCompositeFieldContainer[ErrorFieldViolation]
    def __init__(self, backend: _Optional[str] = ..., operation: _Optional[str] = ..., capability_required: _Optional[str] = ..., retryable: bool = ..., retry_after_ms: _Optional[int] = ..., policy_decision_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., kind: _Optional[_Union[ErrorKind, str]] = ..., field_violations: _Optional[_Iterable[_Union[ErrorFieldViolation, _Mapping]]] = ...) -> None: ...
