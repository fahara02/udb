"""Recovery after a broker restart.

A client must outlive the broker it talks to. Three things kept a client broken
after the broker restarted, until its process was restarted:

* The channel used gRPC's default reconnect backoff, which grows to 120s. Now
  it is capped at 5s (see :mod:`udb_client._channel`). grpcio has no public way
  to cut a running backoff short, so the cap bounds how long a channel that
  failed a few dials keeps failing calls with ``UNAVAILABLE``.
* The owned bearer was renewed only when its local expiry came near. A broker
  that refused it (sessions lost in the restart, a revoked session) got the same
  refused bearer on every call until it expired. Now a refusal of the owned
  bearer renews it (refresh, else sign in again; a service account re-exchanges
  its key) and repeats the call once. The broker refuses authentication before
  any handler runs, so the repeat cannot apply a write twice. AuthnService calls
  are never repeated (their refusals are about the credential in the request),
  and a bearer renewed moments ago that is refused again is not renewed again.
* ``connect_enterprise`` gave up on the first ``UNAVAILABLE`` login. It now
  retries a transient failure, like a renewal does.
"""

from __future__ import annotations

import collections
import random
import threading
import weakref
from typing import Any

import grpc

#: How soon after renewing a refused bearer another refusal is taken as final
#: rather than as a reason to renew again (renewing cannot help).
REJECTED_RENEW_INTERVAL_S = 5.0
#: Attempts and backoff of a sign-in that fails transiently.
LOGIN_ATTEMPTS = 3
LOGIN_BACKOFF_S = 1.0
BACKOFF_CAP_S = 60.0
AUTHN_SERVICE_PREFIX = "/udb.core.authn.services.v1.AuthnService/"

_TRANSIENT = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
    }
)


def jittered_backoff(failures: int, base: float = LOGIN_BACKOFF_S, cap: float = BACKOFF_CAP_S) -> float:
    """The wait after the n-th consecutive failure: a random time in
    ``[ceiling/2, ceiling]``, where ``ceiling = base * 2**(n-1)`` capped at ``cap``."""
    ceiling = min(base * 2 ** max(failures - 1, 0), cap)
    return ceiling / 2 + random.random() * ceiling / 2


def status_code(error: BaseException) -> grpc.StatusCode | None:
    """The gRPC status of ``error``: a raw ``grpc.RpcError`` or a ``UdbRpcError``."""
    code = getattr(error, "code", None)
    if callable(code):
        try:
            code = code()
        except Exception:  # noqa: BLE001 - not a status-carrying error
            return None
    return code if isinstance(code, grpc.StatusCode) else None


def transient_login_error(error: BaseException) -> bool:
    """Whether a failed sign-in may succeed when repeated: the broker was
    unreachable or restarting, overloaded, or the reply was lost."""
    return status_code(error) in _TRANSIENT


class _CallDetails(
    collections.namedtuple(
        "_CallDetails", ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression")
    ),
    grpc.ClientCallDetails,
):
    pass


def _authorization(metadata: Any) -> str:
    values = [value for key, value in (metadata or ()) if key == "authorization"]
    return values[0] if len(values) == 1 and isinstance(values[0], str) else ""


class OwnedBearerInterceptor(grpc.UnaryUnaryClientInterceptor):
    """Repeat, once, a unary call whose owned bearer the broker refused.

    Installed on every synchronous channel a project opens. ``owner`` is the
    project; it decides whether the refused bearer is one it owns and, when it
    is, marks it for renewal. The repeat carries the renewed bearer. A renewal
    that fails refuses the call locally with the owner's credential error.
    """

    def __init__(self, owner: Any):
        self._owner = weakref.ref(owner)

    def intercept_unary_unary(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        outcome = continuation(client_call_details, request)
        owner = self._owner()
        if owner is None:
            return outcome
        try:
            code = outcome.code()
        except Exception:  # noqa: BLE001 - an outcome without a status is returned as is
            return outcome
        if code != grpc.StatusCode.UNAUTHENTICATED:
            return outcome
        method = client_call_details.method
        if isinstance(method, bytes):
            method = method.decode("utf-8", "replace")
        sent = _authorization(client_call_details.metadata)
        if not owner._bearer_rejected(method, sent):
            return outcome
        bearer = owner._current_bearer()
        metadata = [item for item in (client_call_details.metadata or ()) if item[0] != "authorization"]
        metadata.append(("authorization", bearer))
        details = _CallDetails(
            client_call_details.method,
            client_call_details.timeout,
            tuple(metadata),
            client_call_details.credentials,
            getattr(client_call_details, "wait_for_ready", None),
            getattr(client_call_details, "compression", None),
        )
        return continuation(details, request)


class WakeableStop(threading.Event):
    """A loop's stop flag. Setting it also sets ``wake``, the event the loop waits
    on, so a loop that can be woken early still stops at once."""

    def __init__(self, wake: threading.Event):
        super().__init__()
        self.wake = wake

    def set(self) -> None:
        super().set()
        self.wake.set()
