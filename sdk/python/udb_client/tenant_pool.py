"""One connected client per tenant, for a service that serves several tenants."""

from __future__ import annotations

import threading
from dataclasses import replace
from typing import Any, Callable, Mapping

from .connect_env import Expect
from .exceptions import UdbConfigurationError, UdbError
from .project import UdbConfig, UdbProject


class _PoolDial:
    def __init__(self) -> None:
        self.done = threading.Event()
        self.completed = False  # guarded by the pool lock; results never change after done
        self.client: Any = None
        self.error: BaseException | None = None


def _evicted(tenant_id: str) -> UdbError:
    return UdbError(f"udb: tenant pool session for tenant {tenant_id!r} was evicted")


def _closed() -> UdbError:
    return UdbError("udb: tenant pool is closed")


def _close_in_background(client: Any) -> None:
    # A failed client must not live until its replacement connects, and a blocked
    # close must not hold up the replacement.
    if client is not None:
        threading.Thread(target=client.close, name="udb-tenant-pool-close", daemon=True).start()


class TenantSessionPool:
    """Clients are created on first use, shared afterwards, and replaced when their
    credential stops refreshing. Concurrent first calls for a tenant share one connect.

    ``connect`` runs on a worker thread and returns a client with ``close()`` and
    ``credential_error()``, normally a :class:`UdbProject`.
    """

    def __init__(self, connect: Callable[[str], Any]):
        self._connect = connect
        self._lock = threading.Lock()
        self._sessions: dict[str, Any] = {}
        self._pending: dict[str, _PoolDial] = {}
        self._closed = False

    def get(self, tenant_id: str, *, timeout: float | None = None) -> Any:
        """The tenant's client, connecting it on first use.

        ``timeout`` bounds this caller's wait and raises :class:`TimeoutError`. A
        caller that started the connect also abandons it, and a client that arrives
        late is closed. A caller that joined someone else's connect only stops
        waiting. ``connect``'s exception is raised to every waiter.
        """
        with self._lock:
            if self._closed:
                raise _closed()
            stale = None
            client = self._sessions.get(tenant_id)
            if client is not None:
                if _credential_error(client) is None:
                    return client
                del self._sessions[tenant_id]
                stale = client
            dial = self._pending.get(tenant_id)
            started = dial is None
            if started:
                if self._connect is None:
                    _close_in_background(stale)
                    raise UdbConfigurationError("udb: tenant pool connect is required")
                dial = _PoolDial()
                self._pending[tenant_id] = dial
                threading.Thread(
                    target=self._run_dial,
                    args=(tenant_id, dial),
                    name="udb-tenant-pool-dial",
                    daemon=True,
                ).start()
        _close_in_background(stale)
        return self._wait(tenant_id, dial, timeout, started)

    def _wait(self, tenant_id: str, dial: _PoolDial, timeout: float | None, started: bool) -> Any:
        if not dial.done.wait(timeout):
            error = TimeoutError(f"udb: tenant pool connect for tenant {tenant_id!r} timed out")
            if started:
                with self._lock:
                    self._complete_locked(tenant_id, dial, None, error)
            raise error
        with self._lock:
            if dial.error is not None:
                raise dial.error
            if self._closed:
                raise _closed()
            if self._sessions.get(tenant_id) is not dial.client:
                raise _evicted(tenant_id)
            return dial.client

    def _complete_locked(
        self, tenant_id: str, dial: _PoolDial, client: Any, error: BaseException | None
    ) -> None:
        # Every result is fenced by the exact pending object: eviction, closure and
        # timeout publish the refusal now, and the late worker cannot install a
        # client or remove a replacement dial.
        if dial.completed:
            return
        if self._pending.get(tenant_id) is dial:
            del self._pending[tenant_id]
        dial.client, dial.error, dial.completed = client, error, True
        dial.done.set()

    def _run_dial(self, tenant_id: str, dial: _PoolDial) -> None:
        client: Any = None
        error: BaseException | None = None
        try:
            client = self._connect(tenant_id)
            if client is None:
                error = UdbError("udb: tenant pool connect returned no client")
        except Exception as exc:  # noqa: BLE001 - raised to every waiter
            error = exc
        installed = False
        with self._lock:
            if not dial.completed and self._pending.get(tenant_id) is dial:
                if error is None and self._closed:
                    error = _closed()
                if error is None:
                    self._sessions[tenant_id] = client
                    installed = True
                self._complete_locked(tenant_id, dial, client if installed else None, error)
        # Closing can do transport cleanup, so it never runs under the pool lock.
        if not installed and client is not None:
            client.close()

    def tenants(self) -> list[str]:
        """The tenants with a live client, sorted."""
        with self._lock:
            return sorted(self._sessions)

    def evict(self, tenant_id: str) -> None:
        """Close and forget one tenant's client; the next :meth:`get` reconnects."""
        with self._lock:
            client = self._sessions.pop(tenant_id, None)
            dial = self._pending.get(tenant_id)
            if dial is not None:
                self._complete_locked(tenant_id, dial, None, _evicted(tenant_id))
        if client is not None:
            client.close()

    def close(self) -> None:
        """Close every client. Later :meth:`get` calls raise."""
        with self._lock:
            self._closed = True
            sessions = list(self._sessions.values())
            self._sessions = {}
            for tenant_id, dial in list(self._pending.items()):
                self._complete_locked(tenant_id, dial, None, _closed())
        first: BaseException | None = None
        for client in sessions:
            try:
                client.close()
            except Exception as exc:  # noqa: BLE001 - every client still gets closed
                first = first or exc
        if first is not None:
            raise first

    def __enter__(self) -> "TenantSessionPool":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _credential_error(client: Any) -> BaseException | None:
    check = getattr(client, "credential_error", None)
    return check() if check is not None else None


def api_key_tenant_pool(base: UdbConfig, keys: Mapping[str, str]) -> TenantSessionPool:
    """A pool where each tenant connects with ``base`` and its own API key.

    ``keys`` maps tenant UUID to key. A tenant without a key is refused, and a key
    that verifies as another tenant is refused and its connection closed.
    """
    owned = dict(keys)

    def connect(tenant_id: str) -> UdbProject:
        key = owned.get(tenant_id, "")
        if not key:
            raise UdbConfigurationError(f"udb: tenant pool has no API key for tenant {tenant_id!r}")
        project = UdbProject(replace(base, tenant_id=tenant_id, api_key=key, bearer_token=""))
        try:
            project.verify(Expect(tenant_id=tenant_id))
        except BaseException:
            project.close()
            raise
        return project

    return TenantSessionPool(connect)
