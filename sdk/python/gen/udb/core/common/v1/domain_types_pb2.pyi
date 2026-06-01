import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PersonGender(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PERSON_GENDER_UNSPECIFIED: _ClassVar[PersonGender]
    PERSON_GENDER_MALE: _ClassVar[PersonGender]
    PERSON_GENDER_FEMALE: _ClassVar[PersonGender]
    PERSON_GENDER_OTHER: _ClassVar[PersonGender]
    PERSON_GENDER_NOT_PROVIDED: _ClassVar[PersonGender]

class ClientSurface(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLIENT_SURFACE_UNSPECIFIED: _ClassVar[ClientSurface]
    CLIENT_SURFACE_WEB: _ClassVar[ClientSurface]
    CLIENT_SURFACE_IOS: _ClassVar[ClientSurface]
    CLIENT_SURFACE_ANDROID: _ClassVar[ClientSurface]
    CLIENT_SURFACE_API: _ClassVar[ClientSurface]
    CLIENT_SURFACE_ADMIN: _ClassVar[ClientSurface]
    CLIENT_SURFACE_WORKER: _ClassVar[ClientSurface]

class PaymentMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PAYMENT_METHOD_UNSPECIFIED: _ClassVar[PaymentMethod]
    PAYMENT_METHOD_CARD: _ClassVar[PaymentMethod]
    PAYMENT_METHOD_CASH: _ClassVar[PaymentMethod]
    PAYMENT_METHOD_BANK_TRANSFER: _ClassVar[PaymentMethod]
    PAYMENT_METHOD_MOBILE_WALLET: _ClassVar[PaymentMethod]
    PAYMENT_METHOD_ACCOUNT_CREDIT: _ClassVar[PaymentMethod]
    PAYMENT_METHOD_OTHER: _ClassVar[PaymentMethod]
PERSON_GENDER_UNSPECIFIED: PersonGender
PERSON_GENDER_MALE: PersonGender
PERSON_GENDER_FEMALE: PersonGender
PERSON_GENDER_OTHER: PersonGender
PERSON_GENDER_NOT_PROVIDED: PersonGender
CLIENT_SURFACE_UNSPECIFIED: ClientSurface
CLIENT_SURFACE_WEB: ClientSurface
CLIENT_SURFACE_IOS: ClientSurface
CLIENT_SURFACE_ANDROID: ClientSurface
CLIENT_SURFACE_API: ClientSurface
CLIENT_SURFACE_ADMIN: ClientSurface
CLIENT_SURFACE_WORKER: ClientSurface
PAYMENT_METHOD_UNSPECIFIED: PaymentMethod
PAYMENT_METHOD_CARD: PaymentMethod
PAYMENT_METHOD_CASH: PaymentMethod
PAYMENT_METHOD_BANK_TRANSFER: PaymentMethod
PAYMENT_METHOD_MOBILE_WALLET: PaymentMethod
PAYMENT_METHOD_ACCOUNT_CREDIT: PaymentMethod
PAYMENT_METHOD_OTHER: PaymentMethod

class Money(_message.Message):
    __slots__ = ("amount_minor", "currency")
    AMOUNT_MINOR_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    amount_minor: int
    currency: str
    def __init__(self, amount_minor: _Optional[int] = ..., currency: _Optional[str] = ...) -> None: ...

class Address(_message.Message):
    __slots__ = ("address_line1", "address_line2", "locality", "region", "postal_code", "country_code", "latitude", "longitude")
    ADDRESS_LINE1_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_LINE2_FIELD_NUMBER: _ClassVar[int]
    LOCALITY_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    POSTAL_CODE_FIELD_NUMBER: _ClassVar[int]
    COUNTRY_CODE_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    address_line1: str
    address_line2: str
    locality: str
    region: str
    postal_code: str
    country_code: str
    latitude: float
    longitude: float
    def __init__(self, address_line1: _Optional[str] = ..., address_line2: _Optional[str] = ..., locality: _Optional[str] = ..., region: _Optional[str] = ..., postal_code: _Optional[str] = ..., country_code: _Optional[str] = ..., latitude: _Optional[float] = ..., longitude: _Optional[float] = ...) -> None: ...

class ContactInfo(_message.Message):
    __slots__ = ("primary_phone", "email", "alternate_phone", "website")
    PRIMARY_PHONE_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    ALTERNATE_PHONE_FIELD_NUMBER: _ClassVar[int]
    WEBSITE_FIELD_NUMBER: _ClassVar[int]
    primary_phone: str
    email: str
    alternate_phone: str
    website: str
    def __init__(self, primary_phone: _Optional[str] = ..., email: _Optional[str] = ..., alternate_phone: _Optional[str] = ..., website: _Optional[str] = ...) -> None: ...

class GeoPoint(_message.Message):
    __slots__ = ("latitude", "longitude")
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    latitude: float
    longitude: float
    def __init__(self, latitude: _Optional[float] = ..., longitude: _Optional[float] = ...) -> None: ...

class DateRange(_message.Message):
    __slots__ = ("to",)
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    to: _timestamp_pb2.Timestamp
    def __init__(self, to: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., **kwargs) -> None: ...

class ExternalReference(_message.Message):
    __slots__ = ("system", "resource_type", "resource_id", "labels")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SYSTEM_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    system: str
    resource_type: str
    resource_id: str
    labels: _containers.ScalarMap[str, str]
    def __init__(self, system: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ActorReference(_message.Message):
    __slots__ = ("principal_id", "subject", "tenant_id", "project_id", "account_kind", "display_name", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    principal_id: str
    subject: str
    tenant_id: str
    project_id: str
    account_kind: str
    display_name: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, principal_id: _Optional[str] = ..., subject: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., account_kind: _Optional[str] = ..., display_name: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ResourceReference(_message.Message):
    __slots__ = ("resource_type", "resource_id", "resource_name", "tenant_id", "project_id", "backend", "instance", "path", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    resource_type: str
    resource_id: str
    resource_name: str
    tenant_id: str
    project_id: str
    backend: str
    instance: str
    path: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., resource_name: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., backend: _Optional[str] = ..., instance: _Optional[str] = ..., path: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class TagSet(_message.Message):
    __slots__ = ("tags", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TAGS_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    tags: _containers.RepeatedScalarFieldContainer[str]
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, tags: _Optional[_Iterable[str]] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...
