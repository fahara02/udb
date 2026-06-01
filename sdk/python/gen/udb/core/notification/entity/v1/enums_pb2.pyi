from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class NotificationChannel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTIFICATION_CHANNEL_UNSPECIFIED: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_EMAIL: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_SMS: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_PUSH: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_IN_APP: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_WEBHOOK: _ClassVar[NotificationChannel]

class NotificationStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTIFICATION_STATUS_UNSPECIFIED: _ClassVar[NotificationStatus]
    NOTIFICATION_STATUS_PENDING: _ClassVar[NotificationStatus]
    NOTIFICATION_STATUS_SENT: _ClassVar[NotificationStatus]
    NOTIFICATION_STATUS_DELIVERED: _ClassVar[NotificationStatus]
    NOTIFICATION_STATUS_FAILED: _ClassVar[NotificationStatus]
    NOTIFICATION_STATUS_SUPPRESSED: _ClassVar[NotificationStatus]

class NotificationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTIFICATION_TYPE_UNSPECIFIED: _ClassVar[NotificationType]
    NOTIFICATION_TYPE_TRANSACTIONAL: _ClassVar[NotificationType]
    NOTIFICATION_TYPE_SYSTEM: _ClassVar[NotificationType]
    NOTIFICATION_TYPE_MARKETING: _ClassVar[NotificationType]
    NOTIFICATION_TYPE_ALERT: _ClassVar[NotificationType]

class NotificationPriority(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTIFICATION_PRIORITY_UNSPECIFIED: _ClassVar[NotificationPriority]
    NOTIFICATION_PRIORITY_LOW: _ClassVar[NotificationPriority]
    NOTIFICATION_PRIORITY_NORMAL: _ClassVar[NotificationPriority]
    NOTIFICATION_PRIORITY_HIGH: _ClassVar[NotificationPriority]
    NOTIFICATION_PRIORITY_CRITICAL: _ClassVar[NotificationPriority]
NOTIFICATION_CHANNEL_UNSPECIFIED: NotificationChannel
NOTIFICATION_CHANNEL_EMAIL: NotificationChannel
NOTIFICATION_CHANNEL_SMS: NotificationChannel
NOTIFICATION_CHANNEL_PUSH: NotificationChannel
NOTIFICATION_CHANNEL_IN_APP: NotificationChannel
NOTIFICATION_CHANNEL_WEBHOOK: NotificationChannel
NOTIFICATION_STATUS_UNSPECIFIED: NotificationStatus
NOTIFICATION_STATUS_PENDING: NotificationStatus
NOTIFICATION_STATUS_SENT: NotificationStatus
NOTIFICATION_STATUS_DELIVERED: NotificationStatus
NOTIFICATION_STATUS_FAILED: NotificationStatus
NOTIFICATION_STATUS_SUPPRESSED: NotificationStatus
NOTIFICATION_TYPE_UNSPECIFIED: NotificationType
NOTIFICATION_TYPE_TRANSACTIONAL: NotificationType
NOTIFICATION_TYPE_SYSTEM: NotificationType
NOTIFICATION_TYPE_MARKETING: NotificationType
NOTIFICATION_TYPE_ALERT: NotificationType
NOTIFICATION_PRIORITY_UNSPECIFIED: NotificationPriority
NOTIFICATION_PRIORITY_LOW: NotificationPriority
NOTIFICATION_PRIORITY_NORMAL: NotificationPriority
NOTIFICATION_PRIORITY_HIGH: NotificationPriority
NOTIFICATION_PRIORITY_CRITICAL: NotificationPriority
