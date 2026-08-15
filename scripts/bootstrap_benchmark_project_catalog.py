#!/usr/bin/env python3
"""Publish and verify the release catalog for the benchmark customer project.

The live SDK benchmark recreates the database and broker before each SDK. Auth
bootstrap provisions identity only; it does not activate a DataBroker catalog.
Backup, Vault, tenant purge, and other authority-sensitive services intentionally
refuse default-project fallback, so every reset must perform this explicit
StageCatalog -> ActivateCatalog transition before seeding native-service data.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import replace
from pathlib import Path

import grpc

from udb.core.backup.services.v1 import backup_service_pb2 as backup_pb
from udb.core.backup.services.v1 import backup_service_pb2_grpc as backup_grpc
from udb.entity.v1 import admin_pb2
from udb.services.v1 import data_broker_pb2_grpc
from udb_client.auth import UdbAuthClient
from udb_client.metadata import Metadata


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def main() -> int:
    broker_target = required_env("UDB_GRPC_TARGET")
    auth_target = required_env("UDB_AUTH_GRPC_TARGET")
    project_id = required_env("UDB_LIVE_PROJECT")
    tenant_hint = required_env("UDB_LIVE_TENANT")
    username = required_env("UDB_LIVE_USERNAME")
    password = required_env("UDB_LIVE_PASSWORD")
    catalog_path = Path(required_env("UDB_BENCH_CATALOG_MANIFEST"))
    catalog_version = required_env("UDB_BENCH_CATALOG_VERSION")

    login_metadata = Metadata(
        tenant_id=tenant_hint,
        project_id=project_id,
        purpose="ci.benchmark.catalog.bootstrap",
        correlation_id="ci-benchmark-catalog-bootstrap",
        scopes=(),
        service_identity="ci.benchmark.catalog.bootstrap",
        client_catalog_version="",
    )

    with UdbAuthClient(auth_target, login_metadata, timeout=15.0) as auth:
        login = auth.login(username, password, device_name="ci-benchmark-catalog-bootstrap")
        if not login.access_token:
            raise RuntimeError("benchmark login returned no access token")
        principal = auth.authenticate_bearer(login.access_token).principal
        if not principal.tenant_id:
            raise RuntimeError("authenticated benchmark principal returned no canonical tenant")
        if principal.project_id != project_id:
            raise RuntimeError(
                "authenticated benchmark principal is not bound to the requested project: "
                f"claim={principal.project_id!r} requested={project_id!r}"
            )

    metadata = replace(
        login_metadata,
        tenant_id=principal.tenant_id,
        bearer_token=login.access_token,
    )
    context = metadata.to_request_context()
    headers = metadata.to_grpc_metadata()
    broker_channel = grpc.insecure_channel(broker_target)
    auth_channel = grpc.insecure_channel(auth_target)
    broker = data_broker_pb2_grpc.DataBrokerStub(broker_channel)
    # Native services, including Backup, are served on the auth/native listener.
    # The public DataBroker listener correctly returns UNIMPLEMENTED for this RPC.
    backup = backup_grpc.BackupServiceStub(auth_channel)

    try:
        try:
            source = json.loads(catalog_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"cannot read release catalog {catalog_path}: {exc}") from exc
        if not isinstance(source, dict) or not source:
            raise RuntimeError("release catalog export must be a non-empty JSON object")
        source["version"] = catalog_version
        manifest_json = json.dumps(source, sort_keys=True, separators=(",", ":")).encode()

        staged = broker.StageCatalog(
            admin_pb2.StageCatalogRequest(
                context=context,
                manifest_json=manifest_json,
                project_id=project_id,
                reason="activate release catalog for SDK benchmark project",
                idempotency_key=f"benchmark-catalog-stage:{project_id}",
            ),
            metadata=headers,
            timeout=30.0,
        )
        if (
            not staged.catalog_id
            or staged.project_id != project_id
            or not staged.version
            or not staged.checksum_sha256
            or staged.status != "STAGED"
        ):
            raise RuntimeError(
                "StageCatalog returned incomplete or cross-project authority: "
                f"id={staged.catalog_id!r} project={staged.project_id!r} "
                f"version={staged.version!r} checksum={staged.checksum_sha256!r} "
                f"status={staged.status!r}"
            )

        activated = broker.ActivateCatalog(
            admin_pb2.CatalogVersionRequest(
                context=context,
                project_id=project_id,
                version=staged.catalog_id,
                reason="activate release catalog for SDK benchmark project",
                idempotency_key=f"benchmark-catalog-activate:{project_id}",
            ),
            metadata=headers,
            timeout=30.0,
        )
        if (
            activated.catalog_id != staged.catalog_id
            or activated.project_id != project_id
            or activated.version != staged.version
            or activated.checksum_sha256 != staged.checksum_sha256
            or activated.status != "ACTIVE"
        ):
            raise RuntimeError(
                "ActivateCatalog did not return the exact staged ACTIVE binding: "
                f"staged={staged!r} activated={activated!r}"
            )

        durable = broker.GetCatalogVersion(
            admin_pb2.CatalogVersionRequest(
                context=context,
                project_id=project_id,
                version=activated.catalog_id,
            ),
            metadata=headers,
            timeout=20.0,
        )
        if (
            durable.catalog_id != staged.catalog_id
            or durable.project_id != project_id
            or durable.version != staged.version
            or durable.checksum_sha256 != staged.checksum_sha256
            or durable.status != "ACTIVE"
        ):
            raise RuntimeError(
                "durable catalog verification did not match the exact staged ACTIVE identity: "
                f"staged={staged!r} durable={durable!r}"
            )

        # This is the served in-memory preflight. v0.5.8 could report a durable
        # ACTIVE row while accidentally publishing the manifest into `default`;
        # Backup's exact-project guard detects that split before any SDK seed runs.
        backup.ListBackups(
            backup_pb.ListBackupsRequest(tenant_id=principal.tenant_id, page_size=1),
            metadata=headers,
            timeout=20.0,
        )

        print(
            json.dumps(
                {
                    "catalog_id": activated.catalog_id,
                    "catalog_version": activated.version,
                    "checksum_sha256": activated.checksum_sha256,
                    "project_id": project_id,
                    "tenant_id": principal.tenant_id,
                    "status": "ACTIVE_AND_SERVED",
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        broker_channel.close()
        auth_channel.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (grpc.RpcError, RuntimeError) as exc:
        if isinstance(exc, grpc.RpcError):
            print(
                f"benchmark project catalog bootstrap failed: {exc.code().name}: {exc.details()}",
                file=sys.stderr,
            )
        else:
            print(f"benchmark project catalog bootstrap failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
