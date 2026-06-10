from __future__ import annotations

import grpc


class UdbError(Exception):
    """Base exception for the UDB Python SDK."""


class UdbConfigurationError(UdbError):
    """Raised when client configuration is invalid."""


class UdbRpcError(UdbError):
    """Raised when the broker returns a non-OK gRPC status."""

    def __init__(self, rpc_name: str, error: grpc.RpcError):
        self.rpc_name = rpc_name
        self.raw = error
        self.code = error.code()
        self.details = error.details() or ""
        super().__init__(
            f"UDB {rpc_name} failed: gRPC status={self.code.name} details={self.details}"
        )


class UdbPolicyBundleError(UdbError):
    """Raised when a signed policy bundle fails verification.

    Carries the offending :class:`SignedPolicyBundle` (typed as ``object`` to
    avoid importing the generated authz module into this leaf module) so callers
    can inspect ``key_id`` / ``algorithm`` / ``issued_at_unix`` when triaging a
    signature mismatch or an unsupported algorithm.
    """

    def __init__(self, message: str, *, bundle: object | None = None):
        self.bundle = bundle
        super().__init__(f"udb: policy bundle verification failed: {message}")


class UdbAuthzDenied(UdbError):
    """Raised by :meth:`UdbAuthClient.require` when authorization is denied.

    Carries the full server :class:`Decision` so callers can inspect the
    ``deny_reason``, ``required_scopes``, ``matched_policy_ids`` and
    ``decision_id`` without re-issuing the check.
    """

    def __init__(self, resource: str, action: str, decision: object | None = None):
        self.resource = resource
        self.action = action
        # ``decision`` is an authz ``Decision`` proto; typed as ``object`` to
        # avoid importing the generated module into this leaf module.
        self.decision = decision
        reason = getattr(decision, "deny_reason", "") or "access denied"
        super().__init__(f"udb: authz denied for {action} on {resource}: {reason}")
