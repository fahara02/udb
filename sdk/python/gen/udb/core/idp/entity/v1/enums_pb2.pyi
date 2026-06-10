from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class IdpKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    IDP_KIND_UNSPECIFIED: _ClassVar[IdpKind]
    IDP_KIND_NATIVE: _ClassVar[IdpKind]
    IDP_KIND_OIDC: _ClassVar[IdpKind]
    IDP_KIND_SAML: _ClassVar[IdpKind]
    IDP_KIND_LDAP: _ClassVar[IdpKind]
    IDP_KIND_CUSTOM_JWT: _ClassVar[IdpKind]
    IDP_KIND_EXTERNAL_SESSION: _ClassVar[IdpKind]

class AssuranceLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ASSURANCE_LEVEL_UNSPECIFIED: _ClassVar[AssuranceLevel]
    ASSURANCE_LEVEL_NONE: _ClassVar[AssuranceLevel]
    ASSURANCE_LEVEL_LOW: _ClassVar[AssuranceLevel]
    ASSURANCE_LEVEL_SINGLE_FACTOR: _ClassVar[AssuranceLevel]
    ASSURANCE_LEVEL_MULTI_FACTOR: _ClassVar[AssuranceLevel]
    ASSURANCE_LEVEL_HARDWARE: _ClassVar[AssuranceLevel]

class ProviderHealth(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROVIDER_HEALTH_UNSPECIFIED: _ClassVar[ProviderHealth]
    PROVIDER_HEALTH_HEALTHY: _ClassVar[ProviderHealth]
    PROVIDER_HEALTH_DEGRADED: _ClassVar[ProviderHealth]
    PROVIDER_HEALTH_UNREACHABLE: _ClassVar[ProviderHealth]

class DeprovisionPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DEPROVISION_POLICY_UNSPECIFIED: _ClassVar[DeprovisionPolicy]
    DEPROVISION_POLICY_DEACTIVATE: _ClassVar[DeprovisionPolicy]
    DEPROVISION_POLICY_DELETE: _ClassVar[DeprovisionPolicy]
    DEPROVISION_POLICY_RETAIN: _ClassVar[DeprovisionPolicy]
IDP_KIND_UNSPECIFIED: IdpKind
IDP_KIND_NATIVE: IdpKind
IDP_KIND_OIDC: IdpKind
IDP_KIND_SAML: IdpKind
IDP_KIND_LDAP: IdpKind
IDP_KIND_CUSTOM_JWT: IdpKind
IDP_KIND_EXTERNAL_SESSION: IdpKind
ASSURANCE_LEVEL_UNSPECIFIED: AssuranceLevel
ASSURANCE_LEVEL_NONE: AssuranceLevel
ASSURANCE_LEVEL_LOW: AssuranceLevel
ASSURANCE_LEVEL_SINGLE_FACTOR: AssuranceLevel
ASSURANCE_LEVEL_MULTI_FACTOR: AssuranceLevel
ASSURANCE_LEVEL_HARDWARE: AssuranceLevel
PROVIDER_HEALTH_UNSPECIFIED: ProviderHealth
PROVIDER_HEALTH_HEALTHY: ProviderHealth
PROVIDER_HEALTH_DEGRADED: ProviderHealth
PROVIDER_HEALTH_UNREACHABLE: ProviderHealth
DEPROVISION_POLICY_UNSPECIFIED: DeprovisionPolicy
DEPROVISION_POLICY_DEACTIVATE: DeprovisionPolicy
DEPROVISION_POLICY_DELETE: DeprovisionPolicy
DEPROVISION_POLICY_RETAIN: DeprovisionPolicy
