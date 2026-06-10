import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authn.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OTP(_message.Message):
    __slots__ = ("otp_id", "user_id", "otp_type", "code_hash", "delivery_channel", "delivery_address", "status", "attempt_count", "superseded_by_id", "expires_at", "used_at", "created_at", "correlation_id")
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    OTP_TYPE_FIELD_NUMBER: _ClassVar[int]
    CODE_HASH_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    SUPERSEDED_BY_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    USED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    otp_id: str
    user_id: str
    otp_type: _enums_pb2.OTPType
    code_hash: str
    delivery_channel: str
    delivery_address: str
    status: _enums_pb2.OTPStatus
    attempt_count: int
    superseded_by_id: str
    expires_at: _timestamp_pb2.Timestamp
    used_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    correlation_id: str
    def __init__(self, otp_id: _Optional[str] = ..., user_id: _Optional[str] = ..., otp_type: _Optional[_Union[_enums_pb2.OTPType, str]] = ..., code_hash: _Optional[str] = ..., delivery_channel: _Optional[str] = ..., delivery_address: _Optional[str] = ..., status: _Optional[_Union[_enums_pb2.OTPStatus, str]] = ..., attempt_count: _Optional[int] = ..., superseded_by_id: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., used_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., correlation_id: _Optional[str] = ...) -> None: ...
