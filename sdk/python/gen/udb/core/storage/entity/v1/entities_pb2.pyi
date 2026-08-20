from udb.core.storage.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.storage.entity.v1 import file_pb2 as _file_pb2
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar
from udb.core.storage.entity.v1.enums_pb2 import FileType as FileType
from udb.core.storage.entity.v1.enums_pb2 import FileStatus as FileStatus
from udb.core.storage.entity.v1.enums_pb2 import ScanVerdict as ScanVerdict
from udb.core.storage.entity.v1.file_pb2 import File as File

DESCRIPTOR: _descriptor.FileDescriptor
FILE_TYPE_UNSPECIFIED: _enums_pb2.FileType
FILE_TYPE_IMAGE: _enums_pb2.FileType
FILE_TYPE_VIDEO: _enums_pb2.FileType
FILE_TYPE_AUDIO: _enums_pb2.FileType
FILE_TYPE_PDF: _enums_pb2.FileType
FILE_TYPE_DOCUMENT: _enums_pb2.FileType
FILE_TYPE_ARCHIVE: _enums_pb2.FileType
FILE_TYPE_OTHER: _enums_pb2.FileType
FILE_STATUS_UNSPECIFIED: _enums_pb2.FileStatus
FILE_STATUS_PENDING: _enums_pb2.FileStatus
FILE_STATUS_ACTIVE: _enums_pb2.FileStatus
FILE_STATUS_DELETED: _enums_pb2.FileStatus
SCAN_VERDICT_UNSPECIFIED: _enums_pb2.ScanVerdict
SCAN_VERDICT_PENDING: _enums_pb2.ScanVerdict
SCAN_VERDICT_CLEAN: _enums_pb2.ScanVerdict
SCAN_VERDICT_INFECTED: _enums_pb2.ScanVerdict
SCAN_VERDICT_FAILED: _enums_pb2.ScanVerdict
