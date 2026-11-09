"""The one environment contract a service connects with.

With prefix ``UDB_`` (the default):

=======================  =========================================================
``UDB_TARGET``           broker gRPC target, host:port (required)
``UDB_AUTH_TARGET``      control-plane target; defaults to ``UDB_TARGET``
``UDB_WEBRTC_TARGET``    WebRTC signalling target; defaults to the auth target
``UDB_TENANT_ID``        the service's tenant UUID (required)
``UDB_PROJECT_ID``       project (``UDB_PROJECT`` is accepted too)
``UDB_PURPOSE``          purpose sent on every call (required)
``UDB_API_KEY``          service-account key, exchanged for a bearer
                         (required unless ``UDB_BEARER``)
``UDB_BEARER``           a ready bearer token instead of an API key
``UDB_SCOPES``           requested scopes, comma or space separated
``UDB_SERVICE_IDENTITY`` the service identity the grant is bound to
``UDB_DEADLINE``         per-call deadline, a duration such as ``10s`` or ``1m30s``
``UDB_TLS``              ``true`` or ``1`` to dial with TLS (system roots)
``UDB_TLS_SERVER_NAME``  TLS server name override
``UDB_TLS_CA_FILE``      PEM bundle to trust instead of the system roots
``UDB_TLS_CERT_FILE``    PEM client certificate chain for mutual TLS
                         (with ``UDB_TLS_KEY_FILE``)
``UDB_TLS_KEY_FILE``     PEM private key of that client certificate
                         (with ``UDB_TLS_CERT_FILE``)
=======================  =========================================================

Setting any of the three file variables turns TLS on. A file that is missing,
unreadable or holds no PEM is an error naming the variable and path;
``UDB_TLS_CERT_FILE`` and ``UDB_TLS_KEY_FILE`` must be set together.
"""

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass, field
from typing import Sequence

from .exceptions import UdbConfigurationError
from .project import UdbAsyncProject, UdbConfig, UdbProject

_PEM_CERT = re.compile(rb"-----BEGIN CERTIFICATE-----")
_PEM_KEY = re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----")
_DURATION_PART = re.compile(r"((?:\d+(?:\.\d*)?)|(?:\.\d+))(ns|us|µs|μs|ms|s|m|h)")
_DURATION_UNITS = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "μs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}


def _parse_duration(raw: str) -> float | None:
    """Seconds for a duration such as ``10s`` or ``1h30m``; ``None`` when invalid."""
    text = raw[1:] if raw[:1] == "+" else raw
    position = 0
    total = 0.0
    while position < len(text):
        part = _DURATION_PART.match(text, position)
        if part is None:
            return None
        total += float(part.group(1)) * _DURATION_UNITS[part.group(2)]
        position = part.end()
    return total if position > 0 else None


def _split_scopes(raw: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(scope for scope in re.split(r"[, \t]+", raw) if scope))


def config_from_env(prefix: str = "") -> UdbConfig:
    """Read the environment contract into a :class:`UdbConfig`.

    Every missing or invalid variable is reported in one
    :class:`UdbConfigurationError`, so a misconfigured deployment is fixed in one
    pass instead of one restart per variable.
    """
    prefix = prefix or "UDB_"

    def get(name: str) -> str:
        return os.environ.get(prefix + name, "").strip()

    problems: list[str] = []

    def require(name: str) -> str:
        value = get(name)
        if not value:
            problems.append(f"{prefix}{name} is not set")
        return value

    target = require("TARGET")
    tenant_id = require("TENANT_ID")
    purpose = require("PURPOSE")
    api_key = get("API_KEY")
    bearer = get("BEARER")
    if not api_key and not bearer:
        problems.append(f"{prefix}API_KEY (or {prefix}BEARER) is not set")
    auth_target = get("AUTH_TARGET")
    webrtc_target = get("WEBRTC_TARGET")
    for name, value in (("TARGET", target), ("AUTH_TARGET", auth_target), ("WEBRTC_TARGET", webrtc_target)):
        if value and ":" not in value:
            problems.append(f"{prefix}{name} must be host:port, got {value!r}")
    deadline: float | None = 30.0
    raw_deadline = get("DEADLINE")
    if raw_deadline:
        parsed = _parse_duration(raw_deadline)
        if parsed is None or parsed <= 0:
            problems.append(
                f"{prefix}DEADLINE must be a positive duration such as 10s, got {raw_deadline!r}"
            )
        else:
            deadline = parsed

    ca_file, cert_file, key_file = get("TLS_CA_FILE"), get("TLS_CERT_FILE"), get("TLS_KEY_FILE")
    tls = get("TLS").lower() == "true" or get("TLS") == "1" or bool(ca_file or cert_file or key_file)

    # A named file is read now and refused loudly; it never falls back to the
    # system roots or to dialing without the client certificate.
    def read_pem(name: str, path: str, marker: "re.Pattern[bytes]", kind: str) -> bytes | None:
        try:
            with open(path, "rb") as handle:
                data = handle.read()
        except OSError as exc:
            problems.append(f"{prefix}{name}: cannot read {path}: {exc.strerror or exc}")
            return None
        if not marker.search(data):
            problems.append(f"{prefix}{name}: {path} holds no PEM {kind}")
            return None
        return data

    root_certificates: bytes | None = None
    certificate_chain: bytes | None = None
    private_key: bytes | None = None
    if ca_file:
        root_certificates = read_pem("TLS_CA_FILE", ca_file, _PEM_CERT, "certificate")
    if cert_file and key_file:
        certificate_chain = read_pem("TLS_CERT_FILE", cert_file, _PEM_CERT, "certificate")
        private_key = read_pem("TLS_KEY_FILE", key_file, _PEM_KEY, "private key")
    elif cert_file or key_file:
        problems.append(
            f"{prefix}TLS_CERT_FILE and {prefix}TLS_KEY_FILE must be set together "
            "(mutual TLS needs both the client certificate and its key)"
        )
    if problems:
        raise UdbConfigurationError(f"udb: environment is incomplete: {'; '.join(problems)}")

    server_name = get("TLS_SERVER_NAME")
    return UdbConfig(
        target=target,
        auth_target=auth_target,
        webrtc_target=webrtc_target,
        tenant_id=tenant_id,
        project_id=get("PROJECT_ID") or get("PROJECT"),
        purpose=purpose,
        service_identity=get("SERVICE_IDENTITY"),
        scopes=_split_scopes(get("SCOPES")),
        api_key=api_key,
        bearer_token=bearer,
        deadline=deadline,
        tls=tls,
        root_certificates=root_certificates,
        client_certificate_chain=certificate_chain,
        client_private_key=private_key,
        channel_options=(
            (("grpc.ssl_target_name_override", server_name),) if tls and server_name else None
        ),
    )


@dataclass(frozen=True)
class Expect:
    """What a service asserts about the principal it connected as. Every field is optional."""

    tenant_id: str = ""
    project_id: str = ""
    service_identity: str = ""
    #: All must be granted, so a missing scope fails at connect rather than on the
    #: first call that needs it.
    required_scopes: Sequence[str] = field(default_factory=tuple)


def connect_from_env(prefix: str = "", expect: Expect | None = None) -> UdbProject:
    """Read :func:`config_from_env`, connect, and check the principal against ``expect``.

    The project carries an exchanged, self-refreshing bearer when an API key was
    configured. A failed check raises :class:`UdbConfigurationError` and closes the
    connection.
    """
    config = config_from_env(prefix)
    if config.api_key and config.bearer_token:
        raise UdbConfigurationError(
            f"udb: {prefix or 'UDB_'}API_KEY and {prefix or 'UDB_'}BEARER are mutually exclusive"
        )
    project = UdbProject(config)
    try:
        project.verify(expect)
    except BaseException:
        project.close()
        raise
    return project


async def connect_from_env_async(prefix: str = "", expect: Expect | None = None) -> UdbAsyncProject:
    """:func:`connect_from_env` for async code.

    The synchronous project keeps owning the exchanged key and its renewal; the
    returned project sends data and native calls over ``grpc.aio`` with it, and closes
    it. A call that finds the bearer stale waits for the re-exchange on the event loop,
    bounded by the deadline; the background renewal normally gets there first.
    """
    return UdbAsyncProject.from_project(await asyncio.to_thread(connect_from_env, prefix, expect))
