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
