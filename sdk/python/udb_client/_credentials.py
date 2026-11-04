"""Credential refusals raised locally, before a call reaches the broker.

A session whose credential cannot be renewed fails closed: the call is refused
here instead of sending a dead or stale bearer. The refusal has the same shape as
a broker refusal (a :class:`UdbRpcError` with a gRPC status code), so a caller
handling ``UNAUTHENTICATED`` handles both the same way.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field, fields
from typing import Any

import grpc

from .exceptions import UdbError, UdbRpcError
from .metadata import Metadata


class _LocalRpcError(grpc.RpcError):
    """A gRPC status produced in this process rather than by the broker."""

    def __init__(self, code: grpc.StatusCode, details: str):
        super().__init__(details)
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def trailing_metadata(self) -> tuple[()]:
        return ()


class UdbCredentialError(UdbRpcError):
    """The SDK refused a call locally because its credential is not usable."""

    def __init__(self, message: str, *, code: grpc.StatusCode = grpc.StatusCode.UNAUTHENTICATED):
        super().__init__("credential", _LocalRpcError(code, message))
        self.args = (message,)


def credential_refusal(error: BaseException) -> UdbCredentialError:
    """The refusal for a failed renewal: keep its status, else ``UNAUTHENTICATED``."""
    if isinstance(error, UdbCredentialError):
        return error
    code = getattr(error, "code", None)
    if isinstance(code, grpc.StatusCode) and code not in (grpc.StatusCode.OK, grpc.StatusCode.UNKNOWN):
        return UdbCredentialError(f"udb: credential renewal failed: {error}", code=code)
    return UdbCredentialError(f"udb: credential renewal failed: {error}")


def metadata_values(metadata: Metadata) -> dict[str, Any]:
    """The plain :class:`Metadata` fields of ``metadata``, for copying it."""
    return {item.name: getattr(metadata, item.name) for item in fields(Metadata)}


@dataclass(frozen=True)
class DelegatedMetadata(Metadata):
    """Metadata carrying an end user's bearer (see ``UdbProject.as_user``).

    The connected client's own poison and closed checks still apply to the call.
    """

    owner: Any = field(default=None, repr=False, compare=False)

    def to_grpc_metadata(self) -> tuple[tuple[str, str], ...]:
        refusal = getattr(self.owner, "_credential_refusal", None)
        if refusal is not None:
            error = refusal()
            if error is not None:
                raise error
        return super().to_grpc_metadata()


@dataclass(frozen=True)
class OwnedMetadata(Metadata):
    """Metadata whose bearer is resolved from its owner when a call is made.

    Renewal on demand, the poison check and the closed check therefore cover every
    path that sends this metadata: the data client, every native facade, the
    generated clients, and a raw stub given ``project.metadata.to_grpc_metadata()``.
    Derived copies such as ``project.metadata.with_purpose(...)`` behave the same.
    """

    owner: Any = field(default=None, repr=False, compare=False)

    def to_grpc_metadata(self) -> tuple[tuple[str, str], ...]:
        bearer = self.owner._current_bearer()
        headers = [item for item in super().to_grpc_metadata() if item[0] != "authorization"]
        headers.append(("authorization", bearer))
        return tuple(headers)


class UdbCredentialBindingError(UdbError):
    """A renewal verified as a different principal than the client connected as.

    Calls are refused locally rather than sent as someone else.
    """


class RenewalFlight:
    """One renewal. A waiter keeps this result even if a later renewal starts."""

    def __init__(self) -> None:
        self.done = threading.Event()
        self.error: BaseException | None = None


def check_renewed_principal(principal: Any, bound: Any, *expected: Metadata) -> None:
    """Raise :class:`UdbCredentialBindingError` when a renewal changed identity.

    A connected identity cannot change implicitly. ``bound`` is the principal first
    verified, and ``expected`` the metadata the renewal must still match.
    """
    for name, actual, wanted in (
        ("principal_id", principal.principal_id, bound.principal_id),
        ("subject", principal.subject, bound.subject),
        ("account_kind", principal.account_kind, bound.account_kind),
    ):
        if actual != wanted:
            raise UdbCredentialBindingError(f"udb: renewal changed {name}")
    for metadata in expected:
        for name, actual, wanted in (
            ("tenant", principal.tenant_id, metadata.tenant_id),
            ("project", principal.project_id, metadata.project_id),
            ("user", principal.user_id, metadata.user_id),
            ("service identity", principal.service_identity, metadata.service_identity),
        ):
            if actual != wanted:
                raise UdbCredentialBindingError(f"udb: renewed principal changed {name}")
        # Order and duplicates carry no additional permissions.
        if set(principal.scopes) != set(metadata.scopes):
            raise UdbCredentialBindingError("udb: renewed principal changed scopes")


def identity_from_principal(metadata: Metadata, principal: Any, bearer: str) -> Metadata:
    """``metadata`` with its identity taken only from the verified principal.

    Only request and audit configuration stays from what the client connected with.
    """
    values = metadata_values(metadata)
    values.update(
        tenant_id=principal.tenant_id,
        project_id=principal.project_id,
        user_id=principal.user_id,
        service_identity=principal.service_identity,
        scopes=tuple(principal.scopes),
        bearer_token=bearer,
        api_key="",
    )
    return Metadata(**values)


def without_credentials(metadata: Metadata) -> Metadata:
    """Plain metadata for Login, Authenticate and RefreshToken.

    They carry their credential in the typed request, so a recovery attempt never
    presents an expired bearer and never waits on the renewal it is part of.
    """
    values = metadata_values(metadata)
    values.update(bearer_token="", api_key="")
    return Metadata(**values)


def normalized_bearer(bearer: str) -> str:
    """``bearer`` without whitespace or the ``Bearer `` prefix."""
    token = bearer.strip()
    # A bare prefix is no token at all, not a token named "Bearer".
    return "" if token == "Bearer" else token.removeprefix("Bearer ").strip()
