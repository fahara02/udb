from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class FileType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FILE_TYPE_UNSPECIFIED: _ClassVar[FileType]
    FILE_TYPE_IMAGE: _ClassVar[FileType]
    FILE_TYPE_VIDEO: _ClassVar[FileType]
    FILE_TYPE_AUDIO: _ClassVar[FileType]
    FILE_TYPE_PDF: _ClassVar[FileType]
    FILE_TYPE_DOCUMENT: _ClassVar[FileType]
    FILE_TYPE_ARCHIVE: _ClassVar[FileType]
    FILE_TYPE_OTHER: _ClassVar[FileType]

class FileStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FILE_STATUS_UNSPECIFIED: _ClassVar[FileStatus]
    FILE_STATUS_PENDING: _ClassVar[FileStatus]
    FILE_STATUS_ACTIVE: _ClassVar[FileStatus]
    FILE_STATUS_DELETED: _ClassVar[FileStatus]

class ScanVerdict(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SCAN_VERDICT_UNSPECIFIED: _ClassVar[ScanVerdict]
    SCAN_VERDICT_PENDING: _ClassVar[ScanVerdict]
    SCAN_VERDICT_CLEAN: _ClassVar[ScanVerdict]
    SCAN_VERDICT_INFECTED: _ClassVar[ScanVerdict]
    SCAN_VERDICT_FAILED: _ClassVar[ScanVerdict]
FILE_TYPE_UNSPECIFIED: FileType
FILE_TYPE_IMAGE: FileType
FILE_TYPE_VIDEO: FileType
FILE_TYPE_AUDIO: FileType
FILE_TYPE_PDF: FileType
FILE_TYPE_DOCUMENT: FileType
FILE_TYPE_ARCHIVE: FileType
FILE_TYPE_OTHER: FileType
FILE_STATUS_UNSPECIFIED: FileStatus
FILE_STATUS_PENDING: FileStatus
FILE_STATUS_ACTIVE: FileStatus
FILE_STATUS_DELETED: FileStatus
SCAN_VERDICT_UNSPECIFIED: ScanVerdict
SCAN_VERDICT_PENDING: ScanVerdict
SCAN_VERDICT_CLEAN: ScanVerdict
SCAN_VERDICT_INFECTED: ScanVerdict
SCAN_VERDICT_FAILED: ScanVerdict
