from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DirectoryDevice(_message.Message):
    __slots__ = ("tenant_id", "subject_id", "device_id", "signature_public_key", "credential", "key_fingerprint", "status", "registered_at_unix", "revoked_at_unix")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    KEY_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    subject_id: str
    device_id: str
    signature_public_key: bytes
    credential: bytes
    key_fingerprint: str
    status: str
    registered_at_unix: int
    revoked_at_unix: int
    def __init__(self, tenant_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., device_id: _Optional[str] = ..., signature_public_key: _Optional[bytes] = ..., credential: _Optional[bytes] = ..., key_fingerprint: _Optional[str] = ..., status: _Optional[str] = ..., registered_at_unix: _Optional[int] = ..., revoked_at_unix: _Optional[int] = ...) -> None: ...

class RegisterMessagingDeviceRequest(_message.Message):
    __slots__ = ("tenant_id", "subject_id", "device_id", "signature_public_key", "credential")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    subject_id: str
    device_id: str
    signature_public_key: bytes
    credential: bytes
    def __init__(self, tenant_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., device_id: _Optional[str] = ..., signature_public_key: _Optional[bytes] = ..., credential: _Optional[bytes] = ...) -> None: ...

class RegisterMessagingDeviceResponse(_message.Message):
    __slots__ = ("device", "created", "message", "error")
    DEVICE_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    device: DirectoryDevice
    created: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, device: _Optional[_Union[DirectoryDevice, _Mapping]] = ..., created: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListMessagingDevicesRequest(_message.Message):
    __slots__ = ("tenant_id", "subject_id", "include_revoked")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_REVOKED_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    subject_id: str
    include_revoked: bool
    def __init__(self, tenant_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., include_revoked: bool = ...) -> None: ...

class ListMessagingDevicesResponse(_message.Message):
    __slots__ = ("devices", "message", "error")
    DEVICES_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    devices: _containers.RepeatedCompositeFieldContainer[DirectoryDevice]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, devices: _Optional[_Iterable[_Union[DirectoryDevice, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RevokeMessagingDeviceRequest(_message.Message):
    __slots__ = ("tenant_id", "device_id", "reason")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    device_id: str
    reason: str
    def __init__(self, tenant_id: _Optional[str] = ..., device_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RevokeMessagingDeviceResponse(_message.Message):
    __slots__ = ("revoked", "group_ids", "message", "error")
    REVOKED_FIELD_NUMBER: _ClassVar[int]
    GROUP_IDS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    revoked: bool
    group_ids: _containers.RepeatedScalarFieldContainer[str]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, revoked: bool = ..., group_ids: _Optional[_Iterable[str]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class PublishKeyPackagesRequest(_message.Message):
    __slots__ = ("tenant_id", "device_id", "key_packages", "last_resort_key_package")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PACKAGES_FIELD_NUMBER: _ClassVar[int]
    LAST_RESORT_KEY_PACKAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    device_id: str
    key_packages: _containers.RepeatedScalarFieldContainer[bytes]
    last_resort_key_package: bytes
    def __init__(self, tenant_id: _Optional[str] = ..., device_id: _Optional[str] = ..., key_packages: _Optional[_Iterable[bytes]] = ..., last_resort_key_package: _Optional[bytes] = ...) -> None: ...

class PublishKeyPackagesResponse(_message.Message):
    __slots__ = ("accepted", "one_time_remaining", "has_last_resort", "message", "error")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    ONE_TIME_REMAINING_FIELD_NUMBER: _ClassVar[int]
    HAS_LAST_RESORT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    accepted: int
    one_time_remaining: int
    has_last_resort: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, accepted: _Optional[int] = ..., one_time_remaining: _Optional[int] = ..., has_last_resort: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ClaimKeyPackagesRequest(_message.Message):
    __slots__ = ("tenant_id", "requester_device_id", "target_device_ids", "target_subject_ids")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUESTER_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    TARGET_SUBJECT_IDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    requester_device_id: str
    target_device_ids: _containers.RepeatedScalarFieldContainer[str]
    target_subject_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tenant_id: _Optional[str] = ..., requester_device_id: _Optional[str] = ..., target_device_ids: _Optional[_Iterable[str]] = ..., target_subject_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class ClaimedKeyPackage(_message.Message):
    __slots__ = ("subject_id", "device_id", "key_package", "key_package_ref", "last_resort")
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PACKAGE_FIELD_NUMBER: _ClassVar[int]
    KEY_PACKAGE_REF_FIELD_NUMBER: _ClassVar[int]
    LAST_RESORT_FIELD_NUMBER: _ClassVar[int]
    subject_id: str
    device_id: str
    key_package: bytes
    key_package_ref: str
    last_resort: bool
    def __init__(self, subject_id: _Optional[str] = ..., device_id: _Optional[str] = ..., key_package: _Optional[bytes] = ..., key_package_ref: _Optional[str] = ..., last_resort: bool = ...) -> None: ...

class ClaimKeyPackagesResponse(_message.Message):
    __slots__ = ("key_packages", "missing_device_ids", "message", "error")
    KEY_PACKAGES_FIELD_NUMBER: _ClassVar[int]
    MISSING_DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    key_packages: _containers.RepeatedCompositeFieldContainer[ClaimedKeyPackage]
    missing_device_ids: _containers.RepeatedScalarFieldContainer[str]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, key_packages: _Optional[_Iterable[_Union[ClaimedKeyPackage, _Mapping]]] = ..., missing_device_ids: _Optional[_Iterable[str]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetKeyPackageStockRequest(_message.Message):
    __slots__ = ("tenant_id", "device_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    device_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., device_id: _Optional[str] = ...) -> None: ...

class GetKeyPackageStockResponse(_message.Message):
    __slots__ = ("one_time_remaining", "has_last_resort", "message", "error")
    ONE_TIME_REMAINING_FIELD_NUMBER: _ClassVar[int]
    HAS_LAST_RESORT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    one_time_remaining: int
    has_last_resort: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, one_time_remaining: _Optional[int] = ..., has_last_resort: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class CreateGroupRequest(_message.Message):
    __slots__ = ("tenant_id", "group_id", "creator_device_id", "group_info", "ratchet_tree")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    CREATOR_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_INFO_FIELD_NUMBER: _ClassVar[int]
    RATCHET_TREE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    group_id: str
    creator_device_id: str
    group_info: bytes
    ratchet_tree: bytes
    def __init__(self, tenant_id: _Optional[str] = ..., group_id: _Optional[str] = ..., creator_device_id: _Optional[str] = ..., group_info: _Optional[bytes] = ..., ratchet_tree: _Optional[bytes] = ...) -> None: ...

class CreateGroupResponse(_message.Message):
    __slots__ = ("group_id", "epoch", "created", "message", "error")
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    group_id: str
    epoch: int
    created: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, group_id: _Optional[str] = ..., epoch: _Optional[int] = ..., created: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class SubmitCommitRequest(_message.Message):
    __slots__ = ("tenant_id", "group_id", "sender_device_id", "epoch", "commit", "welcome", "group_info", "client_message_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    SENDER_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    COMMIT_FIELD_NUMBER: _ClassVar[int]
    WELCOME_FIELD_NUMBER: _ClassVar[int]
    GROUP_INFO_FIELD_NUMBER: _ClassVar[int]
    CLIENT_MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    group_id: str
    sender_device_id: str
    epoch: int
    commit: bytes
    welcome: bytes
    group_info: bytes
    client_message_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., group_id: _Optional[str] = ..., sender_device_id: _Optional[str] = ..., epoch: _Optional[int] = ..., commit: _Optional[bytes] = ..., welcome: _Optional[bytes] = ..., group_info: _Optional[bytes] = ..., client_message_id: _Optional[str] = ...) -> None: ...

class SubmitCommitResponse(_message.Message):
    __slots__ = ("epoch", "member_device_ids", "added_device_ids", "removed_device_ids", "duplicate", "message", "error")
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    MEMBER_DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    ADDED_DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    REMOVED_DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    epoch: int
    member_device_ids: _containers.RepeatedScalarFieldContainer[str]
    added_device_ids: _containers.RepeatedScalarFieldContainer[str]
    removed_device_ids: _containers.RepeatedScalarFieldContainer[str]
    duplicate: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, epoch: _Optional[int] = ..., member_device_ids: _Optional[_Iterable[str]] = ..., added_device_ids: _Optional[_Iterable[str]] = ..., removed_device_ids: _Optional[_Iterable[str]] = ..., duplicate: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetGroupInfoRequest(_message.Message):
    __slots__ = ("tenant_id", "group_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    group_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., group_id: _Optional[str] = ...) -> None: ...

class GetGroupInfoResponse(_message.Message):
    __slots__ = ("group_id", "epoch", "ratchet_tree", "group_info", "group_info_epoch", "member_device_ids", "message", "error")
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    RATCHET_TREE_FIELD_NUMBER: _ClassVar[int]
    GROUP_INFO_FIELD_NUMBER: _ClassVar[int]
    GROUP_INFO_EPOCH_FIELD_NUMBER: _ClassVar[int]
    MEMBER_DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    group_id: str
    epoch: int
    ratchet_tree: bytes
    group_info: bytes
    group_info_epoch: int
    member_device_ids: _containers.RepeatedScalarFieldContainer[str]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, group_id: _Optional[str] = ..., epoch: _Optional[int] = ..., ratchet_tree: _Optional[bytes] = ..., group_info: _Optional[bytes] = ..., group_info_epoch: _Optional[int] = ..., member_device_ids: _Optional[_Iterable[str]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class SendMessageRequest(_message.Message):
    __slots__ = ("tenant_id", "group_id", "sender_device_id", "ciphertext", "client_message_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    SENDER_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    CLIENT_MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    group_id: str
    sender_device_id: str
    ciphertext: bytes
    client_message_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., group_id: _Optional[str] = ..., sender_device_id: _Optional[str] = ..., ciphertext: _Optional[bytes] = ..., client_message_id: _Optional[str] = ...) -> None: ...

class SendMessageResponse(_message.Message):
    __slots__ = ("epoch", "fanout", "duplicate", "message", "error")
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    FANOUT_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    epoch: int
    fanout: int
    duplicate: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, epoch: _Optional[int] = ..., fanout: _Optional[int] = ..., duplicate: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class InboxEntry(_message.Message):
    __slots__ = ("device_id", "seq", "kind", "group_id", "epoch", "payload", "sender_device_id", "client_message_id", "created_at_unix")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SENDER_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CLIENT_MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    seq: int
    kind: str
    group_id: str
    epoch: int
    payload: bytes
    sender_device_id: str
    client_message_id: str
    created_at_unix: int
    def __init__(self, device_id: _Optional[str] = ..., seq: _Optional[int] = ..., kind: _Optional[str] = ..., group_id: _Optional[str] = ..., epoch: _Optional[int] = ..., payload: _Optional[bytes] = ..., sender_device_id: _Optional[str] = ..., client_message_id: _Optional[str] = ..., created_at_unix: _Optional[int] = ...) -> None: ...

class StreamInboxRequest(_message.Message):
    __slots__ = ("tenant_id", "device_id", "after_seq", "follow", "max_entries")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_SEQ_FIELD_NUMBER: _ClassVar[int]
    FOLLOW_FIELD_NUMBER: _ClassVar[int]
    MAX_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    device_id: str
    after_seq: int
    follow: bool
    max_entries: int
    def __init__(self, tenant_id: _Optional[str] = ..., device_id: _Optional[str] = ..., after_seq: _Optional[int] = ..., follow: bool = ..., max_entries: _Optional[int] = ...) -> None: ...

class StreamInboxResponse(_message.Message):
    __slots__ = ("entry", "error")
    ENTRY_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    entry: InboxEntry
    error: _dto_pb2.ApiError
    def __init__(self, entry: _Optional[_Union[InboxEntry, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class AckInboxRequest(_message.Message):
    __slots__ = ("tenant_id", "device_id", "up_to_seq")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    UP_TO_SEQ_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    device_id: str
    up_to_seq: int
    def __init__(self, tenant_id: _Optional[str] = ..., device_id: _Optional[str] = ..., up_to_seq: _Optional[int] = ...) -> None: ...

class AckInboxResponse(_message.Message):
    __slots__ = ("acked_seq", "acknowledged", "message", "error")
    ACKED_SEQ_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    acked_seq: int
    acknowledged: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, acked_seq: _Optional[int] = ..., acknowledged: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class MessagingBackupInfo(_message.Message):
    __slots__ = ("tenant_id", "subject_id", "backup_id", "size_bytes", "ciphertext_sha256", "version", "created_at_unix", "updated_at_unix")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_SHA256_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    subject_id: str
    backup_id: str
    size_bytes: int
    ciphertext_sha256: str
    version: int
    created_at_unix: int
    updated_at_unix: int
    def __init__(self, tenant_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., backup_id: _Optional[str] = ..., size_bytes: _Optional[int] = ..., ciphertext_sha256: _Optional[str] = ..., version: _Optional[int] = ..., created_at_unix: _Optional[int] = ..., updated_at_unix: _Optional[int] = ...) -> None: ...

class PutMessagingBackupRequest(_message.Message):
    __slots__ = ("tenant_id", "subject_id", "backup_id", "ciphertext", "expected_version")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    subject_id: str
    backup_id: str
    ciphertext: bytes
    expected_version: int
    def __init__(self, tenant_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., backup_id: _Optional[str] = ..., ciphertext: _Optional[bytes] = ..., expected_version: _Optional[int] = ...) -> None: ...

class PutMessagingBackupResponse(_message.Message):
    __slots__ = ("backup", "created", "message", "error")
    BACKUP_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    backup: MessagingBackupInfo
    created: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, backup: _Optional[_Union[MessagingBackupInfo, _Mapping]] = ..., created: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetMessagingBackupRequest(_message.Message):
    __slots__ = ("tenant_id", "subject_id", "backup_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    subject_id: str
    backup_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., backup_id: _Optional[str] = ...) -> None: ...

class GetMessagingBackupResponse(_message.Message):
    __slots__ = ("backup", "ciphertext", "message", "error")
    BACKUP_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    backup: MessagingBackupInfo
    ciphertext: bytes
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, backup: _Optional[_Union[MessagingBackupInfo, _Mapping]] = ..., ciphertext: _Optional[bytes] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DeleteMessagingBackupRequest(_message.Message):
    __slots__ = ("tenant_id", "subject_id", "backup_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    subject_id: str
    backup_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., backup_id: _Optional[str] = ...) -> None: ...

class DeleteMessagingBackupResponse(_message.Message):
    __slots__ = ("deleted", "message", "error")
    DELETED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, deleted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
