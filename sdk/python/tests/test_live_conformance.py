from __future__ import annotations

import importlib
import base64
import json
import os
import pkgutil
import re
import time
import uuid
from dataclasses import replace
from pathlib import Path

import grpc
import pytest
from google.protobuf import struct_pb2
from google.protobuf.descriptor import FieldDescriptor as _FD
from google.protobuf.json_format import ParseDict, ParseError
from google.protobuf.message_factory import GetMessageClass

from udb.services.v1 import data_broker_pb2, data_broker_pb2_grpc
from udb.core.authn.services.v1 import core_pb2 as authn_pb2
from udb.core.authn.services.v1 import authn_service_pb2 as authn_svc_pb2
from udb.core.authn.services.v1 import authn_service_pb2_grpc as authn_grpc
from udb.entity.v1 import admin_pb2, blob_pb2, cdc_pb2, operation_pb2, relational_pb2, stores_pb2, vector_pb2

# Native control-plane service messages + stubs (real CRUD, not just mount probes).
from udb.core.common.v1 import types_pb2 as common_pb, dto_pb2 as common_dto_pb
from udb.core.tenant.services.v1 import tenant_service_pb2 as tenant_pb, tenant_service_pb2_grpc as tenant_grpc
from udb.core.authz.services.v1 import core_pb2 as authz_pb, authz_service_pb2_grpc as authz_grpc
from udb.core.authz.services.v1 import governance_pb2 as authz_gov_pb
from udb.core.idp.entity.v1 import enums_pb2 as idp_enum_pb
from udb.core.idp.services.v1 import core_pb2 as idp_pb
from udb.core.apikey.services.v1 import core_pb2 as apikey_pb, apikey_service_pb2_grpc as apikey_grpc
from udb.core.analytics.services.v1 import core_pb2 as analytics_pb, analytics_service_pb2_grpc as analytics_grpc
from udb.core.notification.services.v1 import core_pb2 as notif_pb, notification_service_pb2_grpc as notif_grpc
from udb.core.storage.services.v1 import storage_service_pb2 as storage_pb, storage_service_pb2_grpc as storage_grpc
from udb.core.asset.services.v1 import asset_service_pb2 as asset_pb, asset_service_pb2_grpc as asset_grpc
from udb.core.webrtc.services.v1 import webrtc_service_pb2 as webrtc_pb, webrtc_service_pb2_grpc as webrtc_grpc
# New-service request messages for the perf seed (Vault/Lock/Workflow/Scheduler/
# Webhook/Backup/Embedding/Search/Metering/Config) — populate the seed refs those
# manifest bodies need (mirrors the Go perf seed).
from udb.core.vault.services.v1 import vault_service_pb2 as vault_pb
from udb.core.lock.services.v1 import lock_service_pb2 as lock_pb
from udb.core.workflow.services.v1 import workflow_service_pb2 as workflow_pb
from udb.core.scheduler.services.v1 import scheduler_service_pb2 as scheduler_pb
from udb.core.webhook.services.v1 import webhook_service_pb2 as webhook_pb
from udb.core.backup.services.v1 import backup_service_pb2 as backup_pb
from udb.core.embedding.services.v1 import embedding_service_pb2 as embedding_pb
from udb.core.search.services.v1 import search_service_pb2 as search_pb
from udb.core.metering.services.v1 import metering_service_pb2 as metering_pb
from udb.core.config.services.v1 import config_service_pb2 as config_pb
from udb.core.control.entity.v1 import enums_pb2 as control_enum_pb
from udb.core.control.services.v1 import core_pb2 as control_pb

from udb_client.auth import UdbAuthClient
from udb_client.generated_client import (
    RPC_API_ALIAS,
    RPC_OPERATION_ID,
    RPC_OPERATION_KIND,
    AnalyticsServiceClient,
    ApiKeyServiceClient,
    AssetServiceClient,
    AuthnServiceClient,
    AuthzServiceClient,
    BackupServiceClient,
    CacheServiceClient,
    ConfigServiceClient,
    ControlPlaneServiceClient,
    DataBrokerClient,
    EmbeddingServiceClient,
    IdentityProviderServiceClient,
    LiveQueryServiceClient,
    LockServiceClient,
    MeteringServiceClient,
    NotificationServiceClient,
    PeerServiceClient,
    RoomServiceClient,
    SchedulerServiceClient,
    SearchServiceClient,
    SignalingServiceClient,
    StorageServiceClient,
    TenantServiceClient,
    TrackServiceClient,
    TurnServiceClient,
    VaultServiceClient,
    WebhookServiceClient,
    WorkflowServiceClient,
)
from udb_client.metadata import Metadata


pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1",
    reason="requires live UDB broker",
)

SERVICE_CLIENTS = [
    AnalyticsServiceClient,
    ApiKeyServiceClient,
    AssetServiceClient,
    AuthnServiceClient,
    AuthzServiceClient,
    BackupServiceClient,
    CacheServiceClient,
    ConfigServiceClient,
    ControlPlaneServiceClient,
    EmbeddingServiceClient,
    IdentityProviderServiceClient,
    LiveQueryServiceClient,
    LockServiceClient,
    MeteringServiceClient,
    NotificationServiceClient,
    SchedulerServiceClient,
    SearchServiceClient,
    StorageServiceClient,
    TenantServiceClient,
    VaultServiceClient,
    WebhookServiceClient,
    PeerServiceClient,
    RoomServiceClient,
    SignalingServiceClient,
    TrackServiceClient,
    TurnServiceClient,
    WorkflowServiceClient,
    DataBrokerClient,
]

FATAL_CODES = {
    grpc.StatusCode.UNIMPLEMENTED,
    grpc.StatusCode.UNAVAILABLE,
    # DEADLINE_EXCEEDED is NOT a mount failure: an unmounted RPC returns
    # UNIMPLEMENTED instantly, so a timeout means the server accepted the call and
    # is processing/blocking (e.g. PublishCDC is an open-ended CDC subscription
    # stream that legitimately blocks waiting for events).
    grpc.StatusCode.UNKNOWN,
}

LIVE_MESSAGE_TYPE = "udb.sdk.live.v1.SdkLiveRecord"


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


def metadata(*, bearer_token: str = "") -> Metadata:
    return Metadata(
        tenant_id=os.getenv("UDB_LIVE_TENANT", "sdk-live"),
        project_id=os.getenv("UDB_LIVE_PROJECT", "default"),
        purpose="python.live.conformance",
        correlation_id="python-live-conformance",
        # No client-asserted scopes: admin authority comes from the Login JWT
        # (broker derives scopes from the validated bearer; header/body scopes are
        # ignored when a JWT verifier is configured). The real production path.
        scopes=(),
        service_identity="python.sdk.live",
        bearer_token=bearer_token,
    )


def live_struct(values: dict) -> struct_pb2.Struct:
    message = struct_pb2.Struct()
    message.update(values)
    return message


def live_record_json(
    record_id: str,
    tenant_id: str,
    project_id: str,
    lookup_key: str,
    payload: str,
    revision: int,
) -> bytes:
    return json.dumps(
        {
            "record_id": record_id,
            "tenant_id": tenant_id,
            "project_id": project_id,
            "lookup_key": lookup_key,
            "payload": payload,
            "revision": revision,
        }
    ).encode()


def mutation_payload(response) -> str:
    return json.loads(bytes(response.record_json).decode())["payload"]


def record_payload(record_set, index: int = 0) -> str:
    return json.loads(bytes(record_set.records_json[index]).decode())["payload"]


def doc_payload(document_set) -> str:
    assert document_set.documents, "DocumentSet must contain at least one document"
    return document_set.documents[0].fields["payload"].string_value


def contains_resource(resources, name: str) -> bool:
    return any(name in resource for resource in resources)


_SERVER_FAULTS = {grpc.StatusCode.INTERNAL, grpc.StatusCode.UNKNOWN, grpc.StatusCode.DATA_LOSS}


def run_live_edge_cases(stub, meta: Metadata) -> None:
    """Per-RPC EDGE cases: malformed/hostile inputs + isolation-boundary probes.

    Every case must FAIL CLOSED with a typed client-side error (or safely
    accept-and-sanitise), never leak another tenant's rows, and never surface a
    server fault (INTERNAL/UNKNOWN/DATA_LOSS = the input crashed the handler
    instead of being validated). Mirrors the Go ``runLiveEdgeCasesE2E`` suite.
    """
    suffix = uuid.uuid4().hex
    ctx = meta.with_purpose("python.live.edge").to_request_context()
    md = meta.to_grpc_metadata()

    # 1. missing project_id in the filter -> project isolation must reject it.
    try:
        stub.Select(relational_pb2.SelectRequest(
            context=ctx, message_type=LIVE_MESSAGE_TYPE,
            filter=live_struct({"tenant_id": meta.tenant_id}), limit=1,
        ), metadata=md, timeout=8.0)
        raise AssertionError("Select without a project_id filter was ACCEPTED — project isolation not enforced")
    except grpc.RpcError as exc:
        assert exc.code() not in _SERVER_FAULTS, f"missing project_id faulted the server ({exc.code()}): {exc.details()}"

    # 2. cross-tenant read -> RLS scopes to the JWT tenant; a foreign filter leaks nothing.
    foreign = "00000000-0000-0000-0000-0000deadbeef"
    try:
        resp = stub.Select(relational_pb2.SelectRequest(
            context=ctx, message_type=LIVE_MESSAGE_TYPE,
            filter=live_struct({"tenant_id": foreign, "project_id": meta.project_id}), limit=10,
        ), metadata=md, timeout=8.0)
        assert len(resp.records_json) == 0, f"cross-tenant Select LEAKED {len(resp.records_json)} record(s) for {foreign}"
    except grpc.RpcError as exc:
        assert exc.code() not in _SERVER_FAULTS, f"cross-tenant Select faulted the server ({exc.code()}): {exc.details()}"

    # 3. NUL byte in a text field -> stripped/rejected, never a raw UTF8 0x00 fault (B14).
    try:
        stub.Upsert(relational_pb2.UpsertRequest(
            context=ctx, message_type=LIVE_MESSAGE_TYPE,
            record_json=live_record_json(
                f"edge-nul-{suffix}", meta.tenant_id, meta.project_id, f"edge-nul-lk-{suffix}", "payload\x00with-nul", 1
            ),
            conflict_fields=["record_id"],
        ), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        assert exc.code() not in _SERVER_FAULTS, f"NUL-byte payload faulted the server ({exc.code()}): {exc.details()}"

    # 4. limit boundaries (negative/zero/huge) -> clamped/validated, never a crash.
    for lim in (-1, 0, 1_000_000):
        try:
            stub.Select(relational_pb2.SelectRequest(
                context=ctx, message_type=LIVE_MESSAGE_TYPE,
                filter=live_struct({"tenant_id": meta.tenant_id, "project_id": meta.project_id}), limit=lim,
            ), metadata=md, timeout=8.0)
        except grpc.RpcError as exc:
            assert exc.code() not in _SERVER_FAULTS, f"Select limit={lim} faulted the server ({exc.code()}): {exc.details()}"

    # 5. unknown message_type -> typed error, not a 500.
    try:
        stub.Select(relational_pb2.SelectRequest(
            context=ctx, message_type="udb.does.not.Exist",
            filter=live_struct({"tenant_id": meta.tenant_id, "project_id": meta.project_id}), limit=1,
        ), metadata=md, timeout=8.0)
        raise AssertionError("Select on an unknown message_type was ACCEPTED")
    except grpc.RpcError as exc:
        assert exc.code() not in _SERVER_FAULTS, f"unknown message_type faulted the server ({exc.code()}): {exc.details()}"

    # 6. invalid backend -> typed error, never a panic/Internal.
    try:
        stub.ListResources(admin_pb2.ResourceAdminRequest(context=ctx, backend="nonexistent-backend-xyz"), metadata=md, timeout=8.0)
        raise AssertionError("ListResources on a nonexistent backend was ACCEPTED")
    except grpc.RpcError as exc:
        assert exc.code() not in _SERVER_FAULTS, f"invalid backend faulted the server ({exc.code()}): {exc.details()}"


def run_live_backend_e2e(stub, meta: Metadata) -> None:
    suffix = uuid.uuid4().hex
    record_id = f"py-{suffix}"
    second_record_id = f"py-batch-{suffix}"
    lookup_key = f"py-live-{suffix}"
    collection = f"sdk_live_docs_{suffix}"
    document_id = f"doc-{suffix}"
    bucket = os.getenv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
    object_key = f"python/{suffix}.txt"
    object_body = f"python live sdk object {suffix}".encode()
    ctx = meta.with_purpose("python.live.backend.e2e").to_request_context()
    md = meta.to_grpc_metadata()

    stub.GenericDispatch(
        admin_pb2.GenericDispatchRequest(
            context=ctx,
            backend="postgres",
            operation="query",
            spec_json='{"sql":"SELECT 1::INT AS live_probe"}',
        ),
        metadata=md,
        timeout=5.0,
    )

    inserted = stub.Upsert(
        relational_pb2.UpsertRequest(
            context=ctx,
            message_type=LIVE_MESSAGE_TYPE,
            record_json=live_record_json(
                record_id, meta.tenant_id, meta.project_id, lookup_key, "created-from-python", 1
            ),
            conflict_fields=["record_id"],
            return_record=True,
        ),
        metadata=md,
        timeout=5.0,
    )
    assert inserted.affected_rows == 1
    assert mutation_payload(inserted) == "created-from-python"

    selected = stub.Select(
        relational_pb2.SelectRequest(
            context=ctx,
            message_type=LIVE_MESSAGE_TYPE,
            filter=live_struct({"record_id": record_id, "tenant_id": meta.tenant_id, "project_id": meta.project_id}),
            limit=1,
        ),
        metadata=md,
        timeout=5.0,
    )
    assert record_payload(selected) == "created-from-python"

    updated = stub.Upsert(
        relational_pb2.UpsertRequest(
            context=ctx,
            message_type=LIVE_MESSAGE_TYPE,
            record_json=live_record_json(
                record_id, meta.tenant_id, meta.project_id, lookup_key, "updated-from-python", 2
            ),
            conflict_fields=["record_id"],
            return_record=True,
        ),
        metadata=md,
        timeout=5.0,
    )
    assert mutation_payload(updated) == "updated-from-python"

    select_v2 = list(
        stub.SelectV2(
            relational_pb2.SelectRequest(
                context=ctx,
                message_type=LIVE_MESSAGE_TYPE,
                filter=live_struct({"record_id": record_id, "tenant_id": meta.tenant_id, "project_id": meta.project_id}),
                limit=1,
            ),
            metadata=md,
            timeout=5.0,
        )
    )
    assert select_v2, "SelectV2 must stream a batch for an existing row"

    batch_upserts = stub.BatchUpsert(
        iter(
            [
                relational_pb2.UpsertRequest(
                    context=ctx,
                    message_type=LIVE_MESSAGE_TYPE,
                    record_json=live_record_json(
                        second_record_id,
                        meta.tenant_id,
                        meta.project_id,
                        f"{lookup_key}-batch",
                        "created-from-python-batch",
                        1,
                    ),
                    conflict_fields=["record_id"],
                )
            ]
        ),
        metadata=md,
        timeout=5.0,
    )
    assert list(batch_upserts), "BatchUpsert must produce a mutation response"
    batch_selects = list(
        stub.BatchSelect(
            iter(
                [
                    relational_pb2.SelectRequest(
                        context=ctx,
                        message_type=LIVE_MESSAGE_TYPE,
                        filter=live_struct({"record_id": second_record_id, "tenant_id": meta.tenant_id, "project_id": meta.project_id}),
                        limit=1,
                    )
                ]
            ),
            metadata=md,
            timeout=5.0,
        )
    )
    assert record_payload(batch_selects[0]) == "created-from-python-batch"

    stub.EnsureResource(
        admin_pb2.ResourceAdminRequest(
            context=ctx,
            backend="mongodb",
            resource_name=collection,
            spec_json=json.dumps({"collection": collection}),
        ),
        metadata=md,
        timeout=5.0,
    )
    resources = stub.ListResources(
        admin_pb2.ResourceAdminRequest(context=ctx, backend="mongodb"),
        metadata=md,
        timeout=5.0,
    )
    assert contains_resource(resources.resources, collection)

    resource = operation_pb2.StoreResource(backend="mongodb", resource_name=collection)
    stub.DocumentUpsert(
        stores_pb2.DocumentUpsertRequest(
            context=ctx,
            resource=resource,
            document_id=document_id,
            document=live_struct(
                {
                    "_id": document_id,
                    "tenant_id": meta.tenant_id,
                    "project_id": meta.project_id,
                    "payload": "mongo-created",
                    "revision": 1,
                }
            ),
        ),
        metadata=md,
        timeout=5.0,
    )
    got_doc = stub.DocumentGet(
        stores_pb2.DocumentGetRequest(context=ctx, resource=resource, document_id=document_id),
        metadata=md,
        timeout=5.0,
    )
    assert doc_payload(got_doc) == "mongo-created"
    stub.DocumentUpsert(
        stores_pb2.DocumentUpsertRequest(
            context=ctx,
            resource=resource,
            document_id=document_id,
            document=live_struct({"payload": "mongo-updated", "revision": 2}),
        ),
        metadata=md,
        timeout=5.0,
    )
    found_doc = stub.DocumentFind(
        stores_pb2.DocumentFindRequest(
            context=ctx,
            resource=resource,
            filter=live_struct({"_id": document_id}),
            limit=1,
        ),
        metadata=md,
        timeout=5.0,
    )
    assert doc_payload(found_doc) == "mongo-updated"
    deleted_doc = stub.DocumentDelete(
        stores_pb2.DocumentDeleteRequest(context=ctx, resource=resource, document_id=document_id),
        metadata=md,
        timeout=5.0,
    )
    assert deleted_doc.affected_rows == 1

    stub.EnsureResource(
        admin_pb2.ResourceAdminRequest(context=ctx, backend="minio", resource_name=bucket, spec_json="{}"),
        metadata=md,
        timeout=5.0,
    )
    put_response = stub.PutObject(
        iter(
            [
                blob_pb2.Chunk(
                    context=ctx,
                    bucket=bucket,
                    object_key=object_key,
                    data=object_body[:10],
                    content_type="text/plain",
                ),
                blob_pb2.Chunk(
                    context=ctx,
                    bucket=bucket,
                    object_key=object_key,
                    data=object_body[10:],
                    final_chunk=True,
                ),
            ]
        ),
        metadata=md,
        timeout=10.0,
    )
    assert put_response.affected_rows == 1
    chunks = list(
        stub.GetObject(
            blob_pb2.ObjectRequest(context=ctx, bucket=bucket, object_key=object_key),
            metadata=md,
            timeout=10.0,
        )
    )
    assert b"".join(chunk.data for chunk in chunks) == object_body
    presigned = stub.GeneratePresignedUrl(
        blob_pb2.UrlRequest(
            context=ctx,
            bucket=bucket,
            object_key=object_key,
            method="GET",
            ttl_seconds=60,
        ),
        metadata=md,
        timeout=5.0,
    )
    assert presigned.url.startswith("http")

    deleted = stub.Delete(
        relational_pb2.DeleteRequest(
            context=ctx,
            message_type=LIVE_MESSAGE_TYPE,
            filter=live_struct({"record_id": record_id, "tenant_id": meta.tenant_id, "project_id": meta.project_id}),
        ),
        metadata=md,
        timeout=5.0,
    )
    assert deleted.affected_rows == 1
    stub.Delete(
        relational_pb2.DeleteRequest(
            context=ctx,
            message_type=LIVE_MESSAGE_TYPE,
            filter=live_struct({"record_id": second_record_id, "tenant_id": meta.tenant_id, "project_id": meta.project_id}),
        ),
        metadata=md,
        timeout=5.0,
    )
    after_delete = stub.Select(
        relational_pb2.SelectRequest(
            context=ctx,
            message_type=LIVE_MESSAGE_TYPE,
            filter=live_struct({"record_id": record_id, "tenant_id": meta.tenant_id, "project_id": meta.project_id}),
            limit=1,
        ),
        metadata=md,
        timeout=5.0,
    )
    assert len(after_delete.records_json) == 0

    # Control-plane data ops with real assertions: project create+list, policy
    # reads, catalog/schema/health. NOTE: PutPolicy is intentionally NOT called —
    # inserting an abac policy flips the data plane to default-deny.
    proj_id = f"sdklive_proj_py_{suffix}"
    stub.EnsureProject(admin_pb2.EnsureProjectRequest(context=ctx, project_id=proj_id, name="SDK Live Project"), metadata=md, timeout=8.0)
    projects = stub.ListProjects(admin_pb2.ProjectListRequest(context=ctx), metadata=md, timeout=8.0)
    assert any(p.project_id == proj_id for p in projects.projects)
    stub.ListPolicies(admin_pb2.PolicyListRequest(context=ctx), metadata=md, timeout=8.0)
    stub.LintPolicies(admin_pb2.CapabilitiesRequest(context=ctx), metadata=md, timeout=8.0)
    manifest = stub.GetCatalogManifest(admin_pb2.CatalogManifestRequest(context=ctx), metadata=md, timeout=8.0)
    assert manifest.manifest_json, "GetCatalogManifest must return a manifest"
    schemas = stub.ListMessageSchemas(admin_pb2.MessageSchemaListRequest(context=ctx, project_id=meta.project_id), metadata=md, timeout=8.0)
    assert len(schemas.message_types) > 0
    lookup = stub.LookupMessageSchema(
        admin_pb2.MessageSchemaLookupRequest(context=ctx, project_id=meta.project_id, message_type=LIVE_MESSAGE_TYPE), metadata=md, timeout=8.0,
    )
    assert lookup.HasField("schema"), f"LookupMessageSchema must resolve {LIVE_MESSAGE_TYPE}"
    stub.GetHealthReport(admin_pb2.HealthReportRequest(context=ctx, with_probes=True, project_id=meta.project_id), metadata=md, timeout=8.0)


NOTIFICATION_CHANNEL_EMAIL = 1  # udb.core.notification.entity.v1.NotificationChannel
API_KEY_STATUS_ACTIVE = 1  # udb.core.apikey.entity.v1.ApiKeyStatus
STORAGE_FILE_TYPE = "DOCUMENT"  # storage rejects unknown file types
# Dev test-mode sentinels: with UDB_WEBAUTHN_TEST_MODE / UDB_SAML_TEST_MODE the broker
# mints + verifies a REAL credential/assertion when the harness sends these (mirrors Go).
WEBAUTHN_TEST_CREDENTIAL = "__UDB_WEBAUTHN_TEST__"
SAML_TEST_SENTINEL = "__UDB_SAML_TEST__"
SAML_IDP_METADATA_XML = (
    '<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/perf-saml">'
    '<md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">'
    '<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/>'
    '<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://idp.example.com/sso"/>'
    "</md:IDPSSODescriptor></md:EntityDescriptor>"
)


def run_native_service_e2e(auth_channel, meta: Metadata, uuid_meta: Metadata | None = None) -> None:
    """Real create→read→assert CRUD against every native control-plane service.

    Tenant-identity fix (auth_fix.md): ``meta.tenant_id`` is the CANONICAL tenant
    UUID (discovered from the authenticated principal), so the UUID-strict services
    (storage/webrtc/asset, which persist tenant_id into a UUID column cross-checked
    against the bearer claim) and the free-text services both accept it under the
    ONE admin. ``uuid_meta`` is retained as an optional override but defaults to
    ``meta``. Authz created_by must be a UUID; notification recipient_id is an FK to
    a real users row.
    """
    if uuid_meta is None:
        uuid_meta = meta
    suffix = uuid.uuid4().hex
    md = meta.to_grpc_metadata()
    wmd = uuid_meta.to_grpc_metadata()
    uuid_tenant = uuid_meta.tenant_id

    # TenantService — CreateTenant is a platform write; Get/Update/List are
    # tenant-self-scoped and the bootstrap admin's tenant has no tenants-table row.
    tenant_stub = tenant_grpc.TenantServiceStub(auth_channel)
    created_tenant = tenant_stub.CreateTenant(
        tenant_pb.CreateTenantRequest(code=f"sdklivepy{suffix}", name="SDK Live Py", type="WORKSPACE"),
        metadata=md, timeout=8.0,
    )
    assert created_tenant.tenant_id, "CreateTenant must return a tenant_id"

    # AuthzService — role create/get/list.
    authz_stub = authz_grpc.AuthzServiceStub(auth_channel)
    role_code = f"sdk_reader_py_{suffix}"
    created_role = authz_stub.CreateRole(
        authz_pb.CreateRoleRequest(
            name=f"SDK Reader Py {suffix}", description="Live SDK reader role",
            created_by=str(uuid.uuid4()), role_code=role_code,
            domain=meta.tenant_id, tenant_id=meta.tenant_id, project_id=meta.project_id,
        ),
        metadata=md, timeout=8.0,
    ).role
    assert created_role.role_code == role_code
    got_role = authz_stub.GetRole(authz_pb.GetRoleRequest(role_id=created_role.role_id), metadata=md, timeout=8.0).role
    assert got_role.role_code == role_code
    roles = authz_stub.ListRoles(authz_pb.ListRolesRequest(domain=meta.tenant_id, active_only=True), metadata=md, timeout=8.0)
    assert any(r.role_id == created_role.role_id for r in roles.roles)

    # Full decision flow: assign the role to a real user, attach an allow policy,
    # prove CheckAccess flips allow→deny across a role revoke (security-critical).
    authn_stub = authn_grpc.AuthnServiceStub(auth_channel)
    subject = authn_stub.CreateUser(
        authn_pb2.CreateUserRequest(
            username=f"sdk-authz-py-{suffix}", email=f"sdk-authz-py-{suffix}@example.com",
            password="CorrectHorse1!", tenant_id=meta.tenant_id, project_id=meta.project_id, full_name="SDK Authz Subject",
        ),
        metadata=md, timeout=8.0,
    ).user
    assigned = authz_stub.AssignRole(
        authz_pb.AssignRoleRequest(
            user_id=subject.user_id, role_id=created_role.role_id, domain=meta.tenant_id,
            assigned_by=subject.user_id, tenant_id=meta.tenant_id, project_id=meta.project_id,
        ),
        metadata=md, timeout=8.0,
    ).user_role
    policy_id = str(uuid.uuid4())
    authz_stub.PutAuthzPolicy(
        authz_pb.PutAuthzPolicyRequest(policy=authz_pb.AuthzPolicyRecord(
            id=policy_id, enabled=True, effect="allow", tenant=meta.tenant_id, project=meta.project_id,
            role=created_role.role_code, action="data.select", resource="invoice",
        )),
        metadata=md, timeout=8.0,
    )
    allowed = authz_stub.CheckAccess(
        authz_pb.CheckAccessRequest(
            user_id=subject.user_id, domain=meta.tenant_id, tenant_id=meta.tenant_id, project_id=meta.project_id,
            object="invoice", action="data.select",
        ),
        metadata=md, timeout=8.0,
    )
    assert allowed.allowed, "CheckAccess must allow the assigned role+policy"
    user_roles = authz_stub.ListUserRoles(
        authz_pb.ListUserRolesRequest(user_id=subject.user_id, domain=meta.tenant_id, active_only=True), metadata=md, timeout=8.0,
    )
    assert len(user_roles.user_roles) == 1
    authz_stub.RevokeRole(
        authz_pb.RevokeRoleRequest(user_role_id=assigned.user_role_id, user_id=subject.user_id, reason="sdk_live_test", revoked_by=subject.user_id),
        metadata=md, timeout=8.0,
    )
    denied = authz_stub.CheckAccess(
        authz_pb.CheckAccessRequest(
            user_id=subject.user_id, domain=meta.tenant_id, tenant_id=meta.tenant_id, project_id=meta.project_id,
            object="invoice", action="data.select",
        ),
        metadata=md, timeout=8.0,
    )
    assert not denied.allowed, "CheckAccess must deny after the role was revoked"

    # ApiKeyService — create/validate/list/revoke lifecycle.
    apikey_stub = apikey_grpc.ApiKeyServiceStub(auth_channel)
    principal = f"sdk-live-svc-{suffix}"
    key_ctx = common_pb.RequestContext(
        user_id=principal,
        tenant=common_pb.TenantContext(tenant_id=meta.tenant_id, project_id=meta.project_id),
    )
    created_key = apikey_stub.CreateApiKey(
        apikey_pb.CreateApiKeyRequest(name=f"sdk-live-key-{suffix}", owner_id=principal, scopes=["data:read"], context=key_ctx),
        metadata=md, timeout=8.0,
    )
    assert created_key.plain_key.startswith("udbk_")
    key_id = created_key.key.key_id
    valid = apikey_stub.ValidateApiKey(
        apikey_pb.ValidateApiKeyRequest(plain_key=created_key.plain_key, required_scope="data:read"), metadata=md, timeout=8.0,
    )
    assert valid.valid and valid.owner_id == principal
    listed_keys = apikey_stub.ListApiKeys(
        apikey_pb.ListApiKeysRequest(owner_id=principal, status=API_KEY_STATUS_ACTIVE), metadata=md, timeout=8.0,
    )
    assert len(listed_keys.keys) == 1 and listed_keys.keys[0].key_id == key_id
    got_key = apikey_stub.GetApiKey(apikey_pb.GetApiKeyRequest(key_id=key_id), metadata=md, timeout=8.0)
    assert got_key.key.owner_id == principal
    apikey_stub.UpdateApiKey(
        apikey_pb.UpdateApiKeyRequest(key_id=key_id, scopes=["data:read", "data:write"], context=key_ctx), metadata=md, timeout=8.0,
    )
    write_ok = apikey_stub.ValidateApiKey(
        apikey_pb.ValidateApiKeyRequest(plain_key=created_key.plain_key, required_scope="data:write"), metadata=md, timeout=8.0,
    )
    assert write_ok.valid, "ValidateApiKey must honor the updated data:write scope"
    apikey_stub.RevokeApiKey(
        apikey_pb.RevokeApiKeyRequest(key_id=key_id, revoke_reason="sdk_live_test", context=key_ctx), metadata=md, timeout=8.0,
    )
    after = apikey_stub.ValidateApiKey(
        apikey_pb.ValidateApiKeyRequest(plain_key=created_key.plain_key, required_scope="data:read"), metadata=md, timeout=8.0,
    )
    assert not after.valid, "revoked API key must not validate"

    # AnalyticsService — record metrics then roll up.
    analytics_stub = analytics_grpc.AnalyticsServiceStub(auth_channel)
    stage = f"sdk_live_stage_py_{suffix}"
    for latency, ok in [(100.0, True), (200.0, True), (400.0, False)]:
        accepted = analytics_stub.RecordPipelineMetric(
            analytics_pb.RecordPipelineMetricRequest(stage_name=stage, tenant_id=meta.tenant_id, latency_ms=latency, is_success=ok),
            metadata=md, timeout=8.0,
        )
        assert accepted.accepted
    summary = analytics_stub.GetPipelineSummary(
        analytics_pb.GetPipelineSummaryRequest(stage_name=stage, tenant_id=meta.tenant_id, page=common_dto_pb.PageRequest(page=1, page_size=10)),
        metadata=md, timeout=8.0,
    )
    assert len(summary.snapshots) == 1 and summary.snapshots[0].total_requests == 3
    throughput = analytics_stub.GetThroughput(analytics_pb.GetThroughputRequest(tenant_id=meta.tenant_id), metadata=md, timeout=8.0)
    assert throughput.total_requests >= 3
    trig = analytics_stub.TriggerSnapshot(analytics_pb.TriggerSnapshotRequest(stage_name=stage), metadata=md, timeout=8.0)
    assert trig.snapshots_written >= 1

    # NotificationService — template + send to a real user (recipient_id FK).
    notif_stub = notif_grpc.NotificationServiceStub(auth_channel)
    authn_stub = authn_grpc.AuthnServiceStub(auth_channel)
    recipient = authn_stub.CreateUser(
        authn_pb2.CreateUserRequest(
            username=f"sdk-notif-py-{suffix}", email=f"sdk-notif-py-{suffix}@example.com",
            password="CorrectHorse1!", tenant_id=meta.tenant_id, project_id=meta.project_id, full_name="SDK Notify Py",
        ),
        metadata=md, timeout=8.0,
    ).user
    event = f"sdk.live.py.{suffix}"
    body = f"sdk-live-body-py-{suffix}"
    notif_stub.UpsertTemplate(
        notif_pb.UpsertTemplateRequest(
            event_type=event, channel=NOTIFICATION_CHANNEL_EMAIL, locale="en",
            subject_template="SDK notify", body_template=body, is_active=True,
        ),
        metadata=md, timeout=8.0,
    )
    template = notif_stub.GetTemplate(
        notif_pb.GetTemplateRequest(event_type=event, channel=NOTIFICATION_CHANNEL_EMAIL, locale="en"), metadata=md, timeout=8.0,
    ).template
    assert template.body_template == body
    sent = notif_stub.SendNotification(
        notif_pb.SendNotificationRequest(
            event_type=event, recipient_id=recipient.user_id, recipient_address=f"sdk+{suffix}@example.com",
            tenant_id=meta.tenant_id, channels=[NOTIFICATION_CHANNEL_EMAIL],
        ),
        metadata=md, timeout=8.0,
    )
    assert sent.logs, "SendNotification must record a log"
    log_id = sent.logs[0].log_id
    listed_notifs = notif_stub.ListNotifications(notif_pb.ListNotificationsRequest(tenant_id=meta.tenant_id), metadata=md, timeout=8.0)
    assert any(l.log_id == log_id for l in listed_notifs.logs)
    got_notif = notif_stub.GetNotification(notif_pb.GetNotificationRequest(log_id=log_id), metadata=md, timeout=8.0)
    assert got_notif.log.log_id == log_id
    notif_stub.SetPreference(
        notif_pb.SetPreferenceRequest(user_id=recipient.user_id, tenant_id=meta.tenant_id, channel=NOTIFICATION_CHANNEL_EMAIL, is_opted_out=True),
        metadata=md, timeout=8.0,
    )
    pref = notif_stub.GetPreference(
        notif_pb.GetPreferenceRequest(user_id=recipient.user_id, tenant_id=meta.tenant_id, channel=NOTIFICATION_CHANNEL_EMAIL),
        metadata=md, timeout=8.0,
    )
    assert pref.preference.is_opted_out, "GetPreference must reflect the opt-out we set"
    prefs = notif_stub.ListPreferences(notif_pb.ListPreferencesRequest(user_id=recipient.user_id, tenant_id=meta.tenant_id), metadata=md, timeout=8.0)
    assert len(prefs.preferences) >= 1
    notif_stub.GetDeliveryStats(notif_pb.GetDeliveryStatsRequest(tenant_id=meta.tenant_id), metadata=md, timeout=8.0)

    # StorageService — file lifecycle under the UUID-tenant admin (project_id and
    # reference_id are UUID columns: empty project → NULL, reference_id a UUID).
    storage_stub = storage_grpc.StorageServiceStub(auth_channel)
    ref = str(uuid.uuid4())
    reg = storage_stub.RegisterUpload(
        storage_pb.RegisterUploadRequest(
            tenant_id=uuid_tenant, project_id="", filename=f"sdk-{suffix}.txt", content_type="text/plain",
            file_type=STORAGE_FILE_TYPE, reference_id=ref, reference_type="sdk.live", size_bytes=128, expires_in_minutes=10,
        ),
        metadata=wmd, timeout=8.0,
    )
    assert reg.file_id and reg.upload_url.startswith("http")
    got_file = storage_stub.GetFile(storage_pb.GetFileRequest(tenant_id=uuid_tenant, file_id=reg.file_id), metadata=wmd, timeout=8.0)
    assert got_file.file.file_id == reg.file_id
    renamed = f"sdk-renamed-{suffix}.txt"
    storage_stub.UpdateFile(storage_pb.UpdateFileRequest(tenant_id=uuid_tenant, file_id=reg.file_id, filename=renamed), metadata=wmd, timeout=8.0)
    reread = storage_stub.GetFile(storage_pb.GetFileRequest(tenant_id=uuid_tenant, file_id=reg.file_id), metadata=wmd, timeout=8.0)
    assert reread.file.filename == renamed, "UpdateFile rename must persist"
    download = storage_stub.GetDownloadUrl(
        storage_pb.GetDownloadUrlRequest(tenant_id=uuid_tenant, file_id=reg.file_id, expires_in_minutes=10), metadata=wmd, timeout=8.0,
    )
    assert download.download_url.startswith("http")
    listed_files = storage_stub.ListFiles(storage_pb.ListFilesRequest(tenant_id=uuid_tenant, reference_id=ref), metadata=wmd, timeout=8.0)
    assert listed_files.total_count >= 1
    deleted_file = storage_stub.DeleteFile(storage_pb.DeleteFileRequest(tenant_id=uuid_tenant, file_id=reg.file_id), metadata=wmd, timeout=8.0)
    assert deleted_file.success

    # AssetService — pipeline definition + asset registered against a stored file.
    asset_stub = asset_grpc.AssetServiceStub(auth_channel)
    asset_file = storage_stub.RegisterUpload(
        storage_pb.RegisterUploadRequest(
            tenant_id=uuid_tenant, project_id="", filename=f"asset-{suffix}.json", content_type="application/json",
            file_type="OTHER", reference_id=str(uuid.uuid4()), reference_type="sdk.asset", size_bytes=64, expires_in_minutes=10,
        ),
        metadata=wmd, timeout=8.0,
    )
    definition = asset_stub.CreatePipelineDefinition(
        asset_pb.CreatePipelineDefinitionRequest(
            tenant_id=uuid_tenant, name=f"sdk-pipeline-{suffix}", description="Live SDK pipeline",
            media_type="application/json", steps='[{"name":"extract","type":"EXTRACT"}]', version=1,
        ),
        metadata=wmd, timeout=8.0,
    )
    assert definition.definition_id
    asset_stub.GetPipelineDefinition(
        asset_pb.GetPipelineDefinitionRequest(tenant_id=uuid_tenant, definition_id=definition.definition_id), metadata=wmd, timeout=8.0,
    )
    asset = asset_stub.RegisterAsset(
        asset_pb.RegisterAssetRequest(
            tenant_id=uuid_tenant, project_id="", file_id=asset_file.file_id, name=f"sdk-asset-{suffix}",
            media_type="application/json", metadata='{"source":"sdk-live"}',
        ),
        metadata=wmd, timeout=8.0,
    )
    assert asset.asset_id
    asset_stub.GetAsset(asset_pb.GetAssetRequest(tenant_id=uuid_tenant, asset_id=asset.asset_id), metadata=wmd, timeout=8.0)
    started = asset_stub.StartPipeline(
        asset_pb.StartPipelineRequest(
            tenant_id=uuid_tenant, definition_id=definition.definition_id, asset_id=asset.asset_id,
            context="{}", correlation_id=f"sdk-live-{suffix}",
        ),
        metadata=wmd, timeout=8.0,
    )
    assert started.instance_id
    asset_stub.GetPipeline(asset_pb.GetPipelineRequest(tenant_id=uuid_tenant, instance_id=started.instance_id), metadata=wmd, timeout=8.0)
    assets = asset_stub.ListAssets(asset_pb.ListAssetsRequest(tenant_id=uuid_tenant), metadata=wmd, timeout=8.0)
    assert any(a.asset_id == asset.asset_id for a in assets.assets)

    # WebRTC — room/peer/track lifecycle + best-effort TURN issuance.
    room_stub = webrtc_grpc.RoomServiceStub(auth_channel)
    peer_stub = webrtc_grpc.PeerServiceStub(auth_channel)
    track_stub = webrtc_grpc.TrackServiceStub(auth_channel)
    turn_stub = webrtc_grpc.TurnServiceStub(auth_channel)
    room = room_stub.CreateRoom(
        webrtc_pb.CreateRoomRequest(tenant_id=uuid_tenant, name=f"sdk-room-{suffix}", max_participants=8, config="{}", created_by=str(uuid.uuid4())),
        metadata=wmd, timeout=8.0,
    )
    assert room.room_id
    room_stub.GetRoom(webrtc_pb.GetRoomRequest(tenant_id=uuid_tenant, room_id=room.room_id), metadata=wmd, timeout=8.0)
    listed_rooms = room_stub.ListRooms(webrtc_pb.ListRoomsRequest(tenant_id=uuid_tenant), metadata=wmd, timeout=8.0)
    assert any(r.room_id == room.room_id for r in listed_rooms.rooms)
    joined = peer_stub.JoinRoom(
        webrtc_pb.JoinRoomRequest(tenant_id=uuid_tenant, room_id=room.room_id, display_name="sdk-peer", metadata="{}", user_agent="sdk-live"),
        metadata=wmd, timeout=8.0,
    )
    assert joined.peer.peer_id
    peer_list = peer_stub.ListPeers(webrtc_pb.ListPeersRequest(tenant_id=uuid_tenant, room_id=room.room_id), metadata=wmd, timeout=8.0)
    assert any(p.peer_id == joined.peer.peer_id for p in peer_list.peers)
    peer_stub.GetPeer(webrtc_pb.GetPeerRequest(tenant_id=uuid_tenant, peer_id=joined.peer.peer_id), metadata=wmd, timeout=8.0)
    room_stub.UpdateRoom(webrtc_pb.UpdateRoomRequest(tenant_id=uuid_tenant, room_id=room.room_id, name=f"sdk-room-renamed-{suffix}"), metadata=wmd, timeout=8.0)
    published = track_stub.PublishTrack(
        webrtc_pb.PublishTrackRequest(tenant_id=uuid_tenant, room_id=room.room_id, peer_id=joined.peer.peer_id, kind="audio", label="mic", settings="{}", metadata="{}"),
        metadata=wmd, timeout=8.0,
    )
    assert published.track_id
    tracks = track_stub.ListTracks(webrtc_pb.ListTracksRequest(tenant_id=uuid_tenant, room_id=room.room_id), metadata=wmd, timeout=8.0)
    assert len(tracks.tracks) >= 1
    track_stub.MuteTrack(webrtc_pb.MuteTrackRequest(tenant_id=uuid_tenant, track_id=published.track_id, muted=True), metadata=wmd, timeout=8.0)
    track_stub.UnpublishTrack(webrtc_pb.UnpublishTrackRequest(tenant_id=uuid_tenant, track_id=published.track_id), metadata=wmd, timeout=8.0)
    try:
        # TURN issuance is best-effort: coturn may be unconfigured locally and the
        # service fail-closes with a real status (not a mount failure).
        turn_stub.IssueCredentials(
            webrtc_pb.IssueCredentialsRequest(tenant_id=uuid_tenant, room_id=room.room_id, peer_id=joined.peer.peer_id, ttl_seconds=3600),
            metadata=wmd, timeout=8.0,
        )
    except grpc.RpcError as exc:
        assert_not_mount_failure("TurnService/IssueCredentials", exc)
    left = peer_stub.LeaveRoom(webrtc_pb.LeaveRoomRequest(tenant_id=uuid_tenant, room_id=room.room_id, peer_id=joined.peer.peer_id), metadata=wmd, timeout=8.0)
    assert left.success
    room_stub.CloseRoom(webrtc_pb.CloseRoomRequest(tenant_id=uuid_tenant, room_id=room.room_id), metadata=wmd, timeout=8.0)


def rpc_path(method) -> str:
    """Full gRPC path "/pkg.Service/Method" for a proto MethodDescriptor."""
    return f"/{method.containing_service.full_name}/{method.name}"


def run_auth_lifecycle(auth_target: str, meta: Metadata, username: str, password: str) -> None:
    """Full session lifecycle: prove Logout invalidates the session — the access
    token, refresh token and session-refresh must ALL fail afterwards. Mirrors the
    Go reference; uses a throwaway login so the admin session is untouched."""
    channel = grpc.insecure_channel(auth_target)
    try:
        authn = authn_grpc.AuthnServiceStub(channel)
        md = meta.to_grpc_metadata()
        login = authn.Login(
            authn_pb2.LoginRequest(username=username, password=password, tenant_hint=meta.tenant_id, project_hint=meta.project_id, device_name="python-sdk-lifecycle"),
            metadata=md, timeout=8.0,
        )
        token, sid, refresh = login.access_token, login.session_id, login.refresh_token
        assert token and sid and refresh, "Login must return access_token+session_id+refresh_token"
        pre = authn.ValidateToken(authn_pb2.ValidateTokenRequest(token=token, token_type=1), metadata=md, timeout=8.0)  # 1 = TOKEN_TYPE_JWT_ACCESS
        assert pre.valid, "fresh access token must validate before logout"
        authn.GetSession(authn_pb2.GetSessionRequest(session_id=sid), metadata=md, timeout=8.0)
        pre_intro = authn.IntrospectToken(authn_pb2.IntrospectTokenRequest(token=token), metadata=md, timeout=8.0)
        assert pre_intro.active, "fresh access token must introspect active before logout"
        out = authn.Logout(authn_pb2.LogoutRequest(session_id=sid, revoke_reason="sdk_live_test"), metadata=md, timeout=8.0)
        assert out.sessions_revoked >= 1, "Logout must revoke at least one session"

        # Post-logout the session is gone: collect EVERY revocation gap in one run.
        failures = []

        # True ⇒ the op still reports the token as live (a revocation GAP). A raised
        # RpcError (correctly denied) or a falsy valid/active counts as revoked.
        # (Prior logic double-negated and flagged a correctly-revoked token as a gap —
        # the broker actually invalidates ValidateToken/IntrospectToken on logout.)
        def _still_live(fn) -> bool:
            try:
                return bool(fn())
            except grpc.RpcError:
                return False

        if _still_live(lambda: authn.ValidateToken(authn_pb2.ValidateTokenRequest(token=token, token_type=1), metadata=md, timeout=8.0).valid):
            failures.append("access token still validates after logout")
        if _still_live(lambda: authn.IntrospectToken(authn_pb2.IntrospectTokenRequest(token=token), metadata=md, timeout=8.0).active):
            failures.append("token still introspects Active after logout")
        try:
            authn.RefreshToken(authn_pb2.RefreshTokenRequest(refresh_token=refresh, session_id=sid), metadata=md, timeout=8.0)
            failures.append("refresh token still works after logout — token family not revoked")
        except grpc.RpcError:
            pass
        try:
            authn.RefreshSession(authn_pb2.RefreshSessionRequest(session_id=sid), metadata=md, timeout=8.0)
            failures.append("RefreshSession still works after logout — session not revoked")
        except grpc.RpcError:
            pass
        # Return (don't assert) so the caller can run the full-surface coverage
        # probe before failing on a logout-revocation gap — mirrors the Go suite,
        # where the lifecycle check is non-fatal and the run still completes.
        return failures
    finally:
        channel.close()


def run_auth_negative(auth_target: str, meta: Metadata, username: str) -> None:
    """Edge cases the happy-path suite skips: the auth plane must fail CLOSED. A
    wrong password must mint no access token, and a garbage/forged bearer must never
    validate or introspect active. A mount failure is still fatal (the negative
    paths must be wired too, not just the positive ones)."""
    channel = grpc.insecure_channel(auth_target)
    try:
        authn = authn_grpc.AuthnServiceStub(channel)
        md = meta.to_grpc_metadata()
        try:
            bad = authn.Login(
                authn_pb2.LoginRequest(
                    username=username, password=f"definitely-wrong-{username}-Pw1!",
                    tenant_hint=meta.tenant_id, project_hint=meta.project_id, device_name="python-sdk-negative",
                ),
                metadata=md, timeout=8.0,
            )
            assert not bad.access_token, "SECURITY: Login with a wrong password returned an access token"
        except grpc.RpcError as exc:
            assert_not_mount_failure("negative Login", exc)
        try:
            v = authn.ValidateToken(authn_pb2.ValidateTokenRequest(token="not-a-real-jwt", token_type=1), metadata=md, timeout=8.0)
            assert not v.valid, "SECURITY: a garbage token validated as valid"
        except grpc.RpcError as exc:
            assert_not_mount_failure("negative ValidateToken", exc)
        try:
            i = authn.IntrospectToken(authn_pb2.IntrospectTokenRequest(token="not-a-real-jwt"), metadata=md, timeout=8.0)
            assert not i.active, "SECURITY: a garbage token introspected as active"
        except grpc.RpcError as exc:
            assert_not_mount_failure("negative IntrospectToken", exc)
    finally:
        channel.close()


UNSUPPORTED_OPERATION_CODE = "UDB_UNSUPPORTED_OPERATION"
# Canonical generic-dispatch op vocabulary the broker gates per backend
# (src/runtime/service/mod.rs check_generic_dispatch_operation). Safe-first so the
# negative probe never picks a destructive op when a safe unclaimed one exists.
GENERIC_DISPATCH_OPS = [
    "ping", "probe", "list_resources", "search", "query",
    "transaction", "get_object", "put_object", "mutate",
    "ensure_resource", "drop_resource",
]


def run_backend_capability_challenge(stub, meta: Metadata, caps) -> None:
    """Challenge EVERY advertised backend's per-operation claims in BOTH directions
    via GenericDispatch (the single op-gated entry point shared by every backend
    kind). A claimed side-effect-free op (ping/probe/list_resources) must be admitted;
    the first unclaimed op must be refused with the declared unsupported code. Proves
    each backend kind honors exactly the surface it advertises."""
    descriptors = list(caps.backend_capabilities)
    assert descriptors, "GetCapabilities advertised zero backend_capabilities descriptors"
    md = meta.to_grpc_metadata()
    ctx = meta.with_purpose("python.live.backend.capability").to_request_context()

    def dispatch(backend: str, op: str):
        try:
            stub.GenericDispatch(
                admin_pb2.GenericDispatchRequest(context=ctx, backend=backend, operation=op, spec_json="{}"),
                metadata=md, timeout=5.0,
            )
            return None
        except grpc.RpcError as exc:
            return exc

    safe_read = {"ping", "probe", "list_resources"}
    for d in descriptors:
        backend = d.backend
        assert backend, "a backend_capabilities descriptor has an empty backend name"
        assert d.tier, f"backend {backend} advertises no tier"
        claimed = set(d.operations)
        assert claimed, f"backend {backend} advertises an empty operations list"
        assert d.unsupported_error_code == UNSUPPORTED_OPERATION_CODE, (
            f"backend {backend} declares unsupported_error_code={d.unsupported_error_code!r}, want {UNSUPPORTED_OPERATION_CODE!r}"
        )
        # Positive: each claimed side-effect-free op must clear the gate.
        for op in safe_read & claimed:
            exc = dispatch(backend, op)
            if exc is not None:
                assert_not_mount_failure(f"{backend}/{op}", exc)
                assert UNSUPPORTED_OPERATION_CODE not in (exc.details() or ""), (
                    f"CAPABILITY LIE: backend {backend} advertises {op} but the gate refused it: {exc.details()}"
                )
        # Negative: the first unclaimed canonical op must be refused with the code.
        for op in GENERIC_DISPATCH_OPS:
            if op in claimed:
                continue
            exc = dispatch(backend, op)
            assert exc is not None, (
                f"CAPABILITY LIE: backend {backend} does NOT advertise {op} yet GenericDispatch admitted it (silent over-claim)"
            )
            assert_not_mount_failure(f"{backend}/{op}", exc)
            assert UNSUPPORTED_OPERATION_CODE in (exc.details() or ""), (
                f"backend {backend} refused unclaimed op {op} but not with {UNSUPPORTED_OPERATION_CODE}: {exc.details()}"
            )
            break

    # NOTE: enabled_backends and backend_capabilities are intentionally NOT
    # cross-checked as a subset relation — they derive from different sources and
    # naming. backend_capabilities is the full compiled matrix (a descriptor per
    # built-in backend, each with a `configured` flag) keyed by canonical name (e.g.
    # "sqlserver"); enabled_backends is the enabled subset, possibly aliased (e.g.
    # "mssql"). The meaningful invariant is the per-backend both-directions op
    # challenge above; a list-vs-list subset assertion flags those legitimate
    # naming/scope differences as false positives.


def _backend_category(tier: str, ops: set) -> str:
    if "get_object" in ops or "put_object" in ops:
        return "object"
    return {
        "vector": "vector", "cache": "cache", "document": "document",
        "graph": "graph", "sql": "relational", "column": "relational",
    }.get(tier.lower(), "")


def run_all_backend_kinds_matrix(stub, meta: Metadata, caps) -> None:
    """Drive a real, category-appropriate data-plane round-trip against EVERY backend
    the broker advertises — relational SQL (GenericDispatch query), object, document,
    cache, vector, graph. The default CI broker runs the relational/document/object
    arms; a richer broker auto-extends to mysql/redis/qdrant/neo4j/etc. A claimed RPC
    must at minimum REACH an implementation (a mount failure is fatal); per-backend
    business quirks are tolerated and values asserted on success."""
    suffix = uuid.uuid4().hex
    md = meta.to_grpc_metadata()

    def ctx(p: str):
        return meta.with_purpose(p).to_request_context()

    def mount_ok(backend: str, op: str, exc) -> None:
        if exc is not None:
            assert_not_mount_failure(f"{backend}/{op}", exc)

    exercised: dict = {}
    for d in caps.backend_capabilities:
        backend = d.backend
        if not backend:
            continue
        ops = set(d.operations)
        cat = _backend_category(d.tier, ops)
        exercised[cat] = exercised.get(cat, 0) + 1
        if cat == "relational":
            try:
                stub.GenericDispatch(
                    admin_pb2.GenericDispatchRequest(
                        context=ctx("python.live.kind.relational"), backend=backend,
                        operation="query", spec_json='{"sql":"SELECT 1 AS live_probe"}',
                    ),
                    metadata=md, timeout=5.0,
                )
            except grpc.RpcError as exc:
                assert_not_mount_failure(f"{backend}/query", exc)
                assert UNSUPPORTED_OPERATION_CODE not in (exc.details() or ""), (
                    f"CAPABILITY LIE: relational backend {backend} refused a claimed query: {exc.details()}"
                )
        elif cat == "object":
            _run_object_kind(stub, md, ctx, backend, suffix, mount_ok)
        elif cat == "document":
            _run_document_kind(stub, md, ctx, backend, suffix, mount_ok)
        elif cat == "cache":
            _run_cache_kind(stub, md, ctx, backend, suffix, mount_ok)
        elif cat == "vector":
            _run_vector_kind(stub, md, ctx, backend, suffix, mount_ok)
        elif cat == "graph":
            _run_graph_kind(stub, md, ctx, backend, suffix, mount_ok)
    assert exercised.get("relational", 0) > 0, "no relational backend advertised — expected at least postgres"
    print(f"\nbackend-kind matrix exercised categories: {exercised}")


def _run_object_kind(stub, md, ctx, backend, suffix, mount_ok) -> None:
    bucket = os.getenv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
    key = f"kind/{backend}/{suffix}.txt"
    body = f"object-kind-{backend}-{suffix}".encode()
    try:
        stub.EnsureResource(admin_pb2.ResourceAdminRequest(context=ctx("python.live.kind.object"), backend=backend, resource_name=bucket, spec_json="{}"), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "ensure_resource", exc)
    try:
        stub.PutObject(iter([blob_pb2.Chunk(context=ctx("python.live.kind.object"), bucket=bucket, object_key=key, data=body, content_type="text/plain", final_chunk=True)]), metadata=md, timeout=10.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "put_object", exc)
        return
    try:
        chunks = list(stub.GetObject(blob_pb2.ObjectRequest(context=ctx("python.live.kind.object"), bucket=bucket, object_key=key), metadata=md, timeout=10.0))
        got = b"".join(c.data for c in chunks)
        if got:
            assert got == body, f"object backend {backend} round-trip body mismatch"
    except grpc.RpcError as exc:
        mount_ok(backend, "get_object", exc)


def _run_document_kind(stub, md, ctx, backend, suffix, mount_ok) -> None:
    collection = f"sdk_kind_docs_{backend.replace('-', '_')}_{suffix}"
    doc_id = f"doc-{suffix}"
    resource = operation_pb2.StoreResource(backend=backend, resource_name=collection)
    try:
        stub.EnsureResource(admin_pb2.ResourceAdminRequest(context=ctx("python.live.kind.document"), backend=backend, resource_name=collection, spec_json=json.dumps({"collection": collection})), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "ensure_resource", exc)
    try:
        stub.DocumentUpsert(stores_pb2.DocumentUpsertRequest(context=ctx("python.live.kind.document"), resource=resource, document_id=doc_id, document=live_struct({"_id": doc_id, "payload": f"doc-{backend}", "revision": 1})), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "mutate", exc)
        return
    try:
        got = stub.DocumentGet(stores_pb2.DocumentGetRequest(context=ctx("python.live.kind.document"), resource=resource, document_id=doc_id), metadata=md, timeout=8.0)
        if got.documents:
            assert doc_payload(got) == f"doc-{backend}", f"document backend {backend} payload mismatch"
    except grpc.RpcError as exc:
        mount_ok(backend, "query", exc)
    try:
        stub.DocumentDelete(stores_pb2.DocumentDeleteRequest(context=ctx("python.live.kind.document"), resource=resource, document_id=doc_id), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "mutate", exc)


def _run_cache_kind(stub, md, ctx, backend, suffix, mount_ok) -> None:
    res = operation_pb2.StoreResource(backend=backend)
    key = f"sdk-live-cache-{suffix}"
    val = f"cache-{backend}-{suffix}".encode()
    try:
        stub.CacheSet(stores_pb2.CacheSetRequest(context=ctx("python.live.kind.cache"), resource=res, key=key, value=val, content_type="text/plain", ttl_seconds=60), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "cache_set", exc)
        return
    try:
        got = stub.CacheGet(stores_pb2.CacheGetRequest(context=ctx("python.live.kind.cache"), resource=res, key=key), metadata=md, timeout=8.0)
        if got.found:
            assert bytes(got.value) == val, f"cache backend {backend} CacheGet mismatch"
    except grpc.RpcError as exc:
        mount_ok(backend, "cache_get", exc)
    try:
        stub.CacheScan(stores_pb2.CacheScanRequest(context=ctx("python.live.kind.cache"), resource=res, key_pattern="sdk-live-cache-*", limit=10), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "cache_scan", exc)
    try:
        stub.CacheDelete(stores_pb2.CacheDeleteRequest(context=ctx("python.live.kind.cache"), resource=res, key=key), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "cache_delete", exc)


def _run_vector_kind(stub, md, ctx, backend, suffix, mount_ok) -> None:
    collection = f"sdk_kind_vec_{backend.replace('-', '_')}_{suffix}"
    try:
        stub.EnsureResource(admin_pb2.ResourceAdminRequest(context=ctx("python.live.kind.vector"), backend=backend, resource_name=collection, spec_json=json.dumps({"dimension": 4, "distance": "cosine"})), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "ensure_resource", exc)
    vec = [0.1, 0.2, 0.3, 0.4]
    try:
        stub.VectorUpsert(vector_pb2.VectorUpsertRequest(context=ctx("python.live.kind.vector"), collection=collection, points=[vector_pb2.VectorPointMutation(id=f"v-{suffix}", vector=vec, payload=live_struct({"tag": "sdk-live"}))]), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "mutate", exc)
        return
    try:
        stub.VectorSearch(vector_pb2.VectorSearchRequest(context=ctx("python.live.kind.vector"), collection=collection, vector=vec, limit=1, with_payload=True), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "search", exc)


def _run_graph_kind(stub, md, ctx, backend, suffix, mount_ok) -> None:
    res = operation_pb2.StoreResource(backend=backend)
    label = f"SdkLive{suffix}"
    try:
        stub.GraphMutate(stores_pb2.GraphMutationRequest(context=ctx("python.live.kind.graph"), resource=res, query=f"CREATE (n:{label} {{id: $id}}) RETURN n", parameters=live_struct({"id": suffix})), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "mutate", exc)
        return
    try:
        stub.GraphQuery(stores_pb2.GraphQueryRequest(context=ctx("python.live.kind.graph"), resource=res, query=f"MATCH (n:{label}) RETURN n LIMIT 1", read_only=True), metadata=md, timeout=8.0)
    except grpc.RpcError as exc:
        mount_ok(backend, "query", exc)


def run_backend_claim_check(caps_stub, meta: Metadata, enabled) -> None:
    """Don't trust GetCapabilities — every advertised backend must answer a real
    ListResources (a mount/unavailable failure means a capability lie)."""
    assert enabled, "GetCapabilities advertised zero backends"
    ctx = meta.to_request_context()
    md = meta.to_grpc_metadata()
    for backend in sorted(enabled):
        try:
            caps_stub.ListResources(admin_pb2.ResourceAdminRequest(context=ctx, backend=backend), metadata=md, timeout=5.0)
        except grpc.RpcError as exc:
            assert_not_mount_failure(f"backend-claim/{backend}", exc)


def service_descriptor(client_cls: type):
    package = importlib.import_module(client_cls._SERVICE_PKG)
    for module in pkgutil.iter_modules(package.__path__):
        if not module.name.endswith("_pb2"):
            continue
        imported = importlib.import_module(f"{client_cls._SERVICE_PKG}.{module.name}")
        descriptor = getattr(imported, "DESCRIPTOR", None)
        if descriptor and client_cls._SERVICE_NAME in descriptor.services_by_name:
            return descriptor.services_by_name[client_cls._SERVICE_NAME]
    raise AssertionError(f"descriptor for {client_cls._SERVICE_FULL} not found")


DOC_BY_CLIENT = {
    "AnalyticsServiceClient": "analytics.md",
    "ApiKeyServiceClient": "apikey.md",
    "AssetServiceClient": "asset.md",
    "AuthnServiceClient": "authn.md",
    "AuthzServiceClient": "authz.md",
    "BackupServiceClient": "backup.md",
    "CacheServiceClient": "cache.md",
    "ConfigServiceClient": "config.md",
    "ControlPlaneServiceClient": "control_plane.md",
    "EmbeddingServiceClient": "embedding.md",
    "IdentityProviderServiceClient": "idp.md",
    "LiveQueryServiceClient": "livequery.md",
    "LockServiceClient": "lock.md",
    "MeteringServiceClient": "metering.md",
    "NotificationServiceClient": "notification.md",
    "SchedulerServiceClient": "scheduler.md",
    "SearchServiceClient": "search.md",
    "StorageServiceClient": "storage.md",
    "TenantServiceClient": "tenant.md",
    "VaultServiceClient": "vault.md",
    "WebhookServiceClient": "webhook.md",
    "PeerServiceClient": "webrtc.md",
    "RoomServiceClient": "webrtc.md",
    "SignalingServiceClient": "webrtc.md",
    "TrackServiceClient": "webrtc.md",
    "TurnServiceClient": "webrtc.md",
    "WorkflowServiceClient": "workflow.md",
    "DataBrokerClient": "data_broker.md",
}

WEBRTC_DOC_PREFIX = {
    "PeerServiceClient": "PeerService",
    "RoomServiceClient": "RoomService",
    "SignalingServiceClient": "SignalingService",
    "TrackServiceClient": "TrackService",
    "TurnServiceClient": "TurnService",
}

INT_FD_TYPES = {
    _FD.TYPE_INT32, _FD.TYPE_INT64, _FD.TYPE_UINT32, _FD.TYPE_UINT64,
    _FD.TYPE_SINT32, _FD.TYPE_SINT64, _FD.TYPE_FIXED32, _FD.TYPE_FIXED64,
    _FD.TYPE_SFIXED32, _FD.TYPE_SFIXED64,
}

DOC_ROWS: dict[tuple[str, str], str] | None = None


def bench_body_rows() -> dict[tuple[str, str], str]:
    """Consume the GENERATED machine-readable manifest
    docs/generated/bench-bodies.json (scripts/gen-bench-bodies-json.mjs). The
    markdown corpus stays the human-editable source; the drift test
    (test_bench_bodies_json_matches_markdown) proves the JSON equals a fresh
    markdown parse, so the two can never diverge. Key is (file, rpc) to match the
    prior markdown-parse shape consumed by doc_body_text."""
    global DOC_ROWS
    if DOC_ROWS is not None:
        return DOC_ROWS
    root = Path(__file__).resolve().parents[3]
    entries = json.loads((root / "docs" / "generated" / "bench-bodies.json").read_text(encoding="utf-8"))
    rows: dict[tuple[str, str], str] = {}
    for e in entries:
        rows[(e["file"], e["rpc"])] = e["body"]
    DOC_ROWS = rows
    return rows


def _parse_bench_body_markdown() -> dict[tuple[str, str], str]:
    """LEGACY markdown parse, retained ONLY to power the drift test that proves the
    generated JSON still equals a fresh parse of the human-editable markdown."""
    root = Path(__file__).resolve().parents[3]
    rows: dict[tuple[str, str], str] = {}
    for path in sorted((root / "docs" / "bench-bodies").glob("*.md")):
        if path.name == "workflow-sequences.md":
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.startswith("| ["):
                continue
            parts = [part.strip() for part in line.strip().strip("|").split("|")]
            if len(parts) >= 5 and parts[1] != "RPC":
                rows[(path.name, parts[1])] = parts[4]
    return rows


def test_bench_bodies_json_matches_markdown() -> None:
    """R6.1 DRIFT gate: docs/generated/bench-bodies.json must equal a fresh parse of
    the human-editable docs/bench-bodies/*.md. Edit markdown without regenerating
    (`node scripts/gen-bench-bodies-json.mjs`) and this fails."""
    from_json = bench_body_rows()
    from_md = _parse_bench_body_markdown()
    expected = len(RPC_OPERATION_KIND)
    assert len(from_json) == expected, f"JSON manifest has {len(from_json)} rows, want {expected}"
    assert len(from_md) == expected, f"markdown manifest has {len(from_md)} rows, want {expected}"
    assert from_json == from_md, (
        "bench-bodies.json drifted from markdown (run `node scripts/gen-bench-bodies-json.mjs`)"
    )


def doc_body_text(client_cls, method) -> str | None:
    filename = DOC_BY_CLIENT[client_cls.__name__]
    method_name = method.name
    prefix = WEBRTC_DOC_PREFIX.get(client_cls.__name__)
    rows = bench_body_rows()
    if prefix:
        return rows.get((filename, f"{prefix}.{method_name}")) or rows.get((filename, method_name))
    service_name = getattr(client_cls, "_SERVICE_NAME", "")
    if service_name:
        return rows.get((filename, f"{service_name}.{method_name}")) or rows.get((filename, method_name))
    return rows.get((filename, method_name))


def doc_field_names(client_cls, method) -> set[str]:
    body = doc_body_text(client_cls, method)
    if body is None:
        raise MissingExplicitPerfBody(f"no docs/bench-bodies row for {client_cls._SERVICE_FULL}/{method.name}")
    low = body.lower()
    fields: set[str] = set()
    for field in method.input_type.fields:
        names = {field.name.lower(), field.json_name.lower()}
        if field.name == "context" and ("ctx" in low or "context" in low):
            fields.add(field.name)
            continue
        if any(re.search(rf"(?<![a-z0-9_]){re.escape(name)}(?![a-z0-9_])", low) for name in names):
            fields.add(field.name)
    if client_cls is DataBrokerClient:
        if method.name in {"BatchSelect", "SelectV2"}:
            fields.update({"context", "message_type", "filter", "limit"})
        elif method.name in {"BatchUpsert"}:
            fields.update({"context", "message_type", "record_json", "conflict_fields"})
        elif method.name in {"VectorBatchUpsert"}:
            fields.update({"context", "collection", "points"})
    return fields


def doc_mentions_field(field, body: str) -> bool:
    low = body.lower()
    names = {field.name.lower(), field.json_name.lower()}
    return any(re.search(rf"(?<![a-z0-9_]){re.escape(name)}(?![a-z0-9_])", low) for name in names)


class MissingExplicitPerfBody(RuntimeError):
    pass


def _doc_body_json(body: str | None):
    if body is None:
        return None
    raw = body.strip()
    if raw.startswith("`") and raw.endswith("`"):
        raw = raw[1:-1].strip()
    if not raw:
        return None
    return json.loads(raw)


def _resolve_doc_json_value(value, meta: Metadata, fix: "PerfFixtures"):
    if isinstance(value, str):
        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            if key in {"tenant_id", "tenant"}:
                return meta.tenant_id
            if key in {"project_id", "project"}:
                return meta.project_id
            seeded = fix.lookup(key)
            if seeded:
                return seeded
            raise MissingExplicitPerfBody(f"no seeded value for <seed:{key}>")

        return re.sub(r"<seed:([^>]+)>", repl, value)
    if isinstance(value, list):
        return [_resolve_doc_json_value(v, meta, fix) for v in value]
    if isinstance(value, dict):
        return {k: _resolve_doc_json_value(v, meta, fix) for k, v in value.items()}
    return value


def perf_real_body(client_cls, method, meta: Metadata, fix: "PerfFixtures"):
    """Build a request from the explicit row for this RPC in docs/bench-bodies."""
    body = doc_body_text(client_cls, method)
    request = GetMessageClass(method.input_type)()
    try:
        parsed = _doc_body_json(body)
        if parsed is None:
            raise ValueError("empty doc body")
        ParseDict(_resolve_doc_json_value(parsed, meta, fix), request, ignore_unknown_fields=False)
    except (json.JSONDecodeError, ParseError, ValueError):
        fields = doc_field_names(client_cls, method)
        apply_doc_fields(request, fields, body or "", meta, fix)
    postprocess_perf_body(client_cls, method, request, meta, fix)
    return request


def postprocess_perf_body(client_cls, method, request, meta: Metadata, fix: "PerfFixtures") -> None:
    """Adjust doc-grounded bodies where the valid body is intentionally per-call.

    Docs give the shape; some create RPCs still need unique natural keys so the
    first measured iteration can succeed instead of colliding with seed data.
    """
    suffix = uuid.uuid4().hex[:12]
    if client_cls is AuthnServiceClient and method.name == "CreateUser":
        request.username = f"py-perf-u-{suffix}"
        request.email = f"py-perf-u-{suffix}@example.com"
        request.password = "Str0ng!Passw0rd"
        request.tenant_id = meta.tenant_id
        if hasattr(request, "project_id"):
            request.project_id = meta.project_id
    elif client_cls is AuthnServiceClient and method.name == "SendOTP":
        request.user_id = _fixture(fix, "send_otp_user_id", _fixture(fix, "user_id", request.user_id))
        if hasattr(request, "otp_type"):
            request.otp_type = 4
    elif client_cls is AuthnServiceClient and method.name == "VerifyOTP":
        request.otp_id = _fixture(fix, "otp_id", request.otp_id)
        request.code = _fixture(fix, "otp_code", request.code)
    elif client_cls is AuthnServiceClient and method.name == "ResetPassword":
        request.otp_id = _fixture(fix, "reset_otp_id", request.otp_id)
        request.code = _fixture(fix, "reset_otp_code", request.code)
        request.new_password = "N3w!Passw0rd9"
    elif client_cls is AuthnServiceClient and method.name == "ResendOTP":
        request.original_otp_id = _fixture(fix, "otp_id", request.original_otp_id)
    elif client_cls is AuthnServiceClient and method.name == "VerifyMfaChallenge":
        request.challenge_id = _fixture(fix, "challenge_id", request.challenge_id)
        request.code = _fixture(fix, "otp_code", request.code)
    elif client_cls is AuthnServiceClient and method.name == "ChangePassword":
        request.current_password = _fixture(fix, "password", request.current_password)
        request.new_password = "N3w!Passw0rd9"
        if hasattr(request, "otp_id"):
            request.otp_id = ""
    elif client_cls is AuthnServiceClient and method.name == "FinishWebAuthnRegistration":
        # Dev soft-authenticator: send the sentinel; the broker mints + verifies a REAL
        # credential against the challenge minted by the seed's StartWebAuthnRegistration.
        request.challenge_id = _fixture(fix, "reg_challenge_id", _fixture(fix, "challenge_id", request.challenge_id))
        request.public_key_credential_json = WEBAUTHN_TEST_CREDENTIAL
    elif client_cls is AuthnServiceClient and method.name == "FinishWebAuthnAuthentication":
        request.challenge_id = _fixture(fix, "auth_challenge_id", _fixture(fix, "challenge_id", request.challenge_id))
        request.public_key_credential_json = WEBAUTHN_TEST_CREDENTIAL
    elif client_cls is AuthnServiceClient and method.name == "RevokeDevice":
        request.device_id = _fixture(fix, "revoke_device_id", _fixture(fix, "device_id", request.device_id))
    elif client_cls is AuthzServiceClient and method.name == "CreateRole":
        request.name = f"SDK Perf Role {suffix}"
        request.role_code = f"py_perf_role_{suffix}"
        request.domain = meta.tenant_id
        request.tenant_id = meta.tenant_id
        request.project_id = meta.project_id
        request.created_by = _fixture(fix, "user_id", request.created_by)
    elif client_cls is AuthzServiceClient and method.name == "UpdatePolicyDraft":
        request.draft_id = _fixture(fix, "update_draft_id", request.draft_id)
        if hasattr(request, "expected_updated_at_unix"):
            request.expected_updated_at_unix = int(_fixture(fix, "update_draft_updated_at_unix", "0") or "0")
    elif client_cls is AuthzServiceClient and method.name == "DiffPolicyDraft":
        request.draft_id = _fixture(fix, "update_draft_id", request.draft_id)
    elif client_cls is AuthzServiceClient and method.name == "ApprovePolicyDraft":
        request.draft_id = _fixture(fix, "approve_draft_id", request.draft_id)
        request.reviewer = _fixture(fix, "user_id", request.reviewer)
    elif client_cls is AuthzServiceClient and method.name == "RejectPolicyDraft":
        request.draft_id = _fixture(fix, "reject_draft_id", request.draft_id)
        request.reviewer = _fixture(fix, "user_id", request.reviewer)
    elif client_cls is AuthzServiceClient and method.name == "ActivatePolicyVersion":
        request.policy_version_id = _fixture(fix, "policy_version_id", request.policy_version_id)
    elif client_cls is AuthzServiceClient and method.name == "ActivateCanary":
        request.policy_version_id = _fixture(fix, "canary_version_id", request.policy_version_id)
    elif client_cls is AuthzServiceClient and method.name in {"GetCanaryStatus", "PromoteCanary"}:
        request.canary_id = _fixture(fix, "canary_id", request.canary_id)
    elif client_cls is AuthzServiceClient and method.name == "RollbackPolicyVersion":
        request.policy_set_id = _fixture(fix, "rollback_policy_set_id", request.policy_set_id)
        request.target_version_id = _fixture(fix, "rollback_target_version_id", request.target_version_id)
    elif client_cls is ApiKeyServiceClient and method.name == "UpdateApiKey":
        request.key_id = _fixture(fix, "update_key_id", request.key_id)
    elif client_cls is ApiKeyServiceClient and method.name == "RevokeApiKey":
        request.key_id = _fixture(fix, "revoke_key_id", request.key_id)
    elif client_cls is StorageServiceClient and method.name == "RegisterUpload":
        request.filename = f"perf-{suffix}.pdf"
        request.project_id = ""
        request.reference_id = str(uuid.uuid4())
        request.reference_type = "document"
    elif client_cls is StorageServiceClient and method.name == "FinalizeUpload":
        # Finalizing the already-finalized primary file_id again fails "upload already
        # finalized", so the measured FinalizeUpload targets a SEPARATE registered+
        # uploaded-but-NOT-finalized file seeded as finalize_file_id.
        request.file_id = _fixture(fix, "finalize_file_id", request.file_id)
        request.reference_id = _fixture(fix, "finalize_file_id", request.reference_id)
        if hasattr(request, "size_bytes"):
            request.size_bytes = int(_fixture(fix, "file_size_bytes", "17"))
        request.content_type = "text/plain"
        request.file_type = STORAGE_FILE_TYPE
        request.reference_type = "sdk.perf"
    elif client_cls is StorageServiceClient and method.name == "DownloadFile":
        # Server-streaming download fallback: the primary file_id is finalized with
        # object bytes present (seeded), so the first DownloadFileChunk delivers.
        request.file_id = _fixture(fix, "file_id", request.file_id)
        request.chunk_size_bytes = 65536
    elif client_cls is AssetServiceClient and method.name == "RegisterAsset":
        if hasattr(request, "project_id"):
            request.project_id = ""
        request.media_type = "image/png"
        request.metadata = '{"source":"upload"}'
    elif client_cls is AssetServiceClient and method.name == "ListAssets":
        request.status = ""
        request.media_type = "image/png"
    elif client_cls is RoomServiceClient and method.name == "CreateRoom":
        request.name = f"bench-room-{suffix}"
        request.config = "{}"
        request.created_by = _fixture(fix, "user_id", request.created_by)
    elif client_cls is IdentityProviderServiceClient and method.name == "CreateProvider":
        request.display_name = f"Acme OIDC {suffix}"
        request.issuer = f"https://idp.example.com/{suffix}"
    elif client_cls is IdentityProviderServiceClient and method.name == "UpdateProvider":
        request.group_mapping_json = json.dumps({_fixture(fix, "scim_group_id", "sdk-perf-group"): "admin"})
    elif client_cls is IdentityProviderServiceClient and method.name == "StartSamlLogin":
        request.provider_id = _fixture(fix, "saml_provider_id", request.provider_id)
    elif client_cls is IdentityProviderServiceClient and method.name == "SamlAcs":
        request.provider_id = _fixture(fix, "saml_provider_id", request.provider_id)
        request.tenant_id = meta.tenant_id
        request.saml_response = SAML_TEST_SENTINEL
        request.relay_state = "state-1"
    elif client_cls is IdentityProviderServiceClient and method.name == "ScimGetGroup":
        request.provider_id = _fixture(fix, "provider_id", request.provider_id)
        request.scim_group_id = _fixture(fix, "scim_group_id", request.scim_group_id)
    elif client_cls is TrackServiceClient and method.name == "UnpublishTrack":
        request.track_id = _fixture(fix, "unpublish_track_id", request.track_id)
    elif client_cls is PeerServiceClient and method.name == "JoinSession":
        # The main room_id is filled to capacity (8) by JoinRoom's mutation iters, so
        # JoinSession there hits "room ... at capacity". Use a SEPARATE high-capacity
        # room seeded as join_session_room_id.
        request.room_id = _fixture(fix, "join_session_room_id", request.room_id)
    elif client_cls is PeerServiceClient and method.name == "LeaveRoom":
        request.peer_id = _fixture(fix, "leave_peer_id", request.peer_id)
    elif client_cls is RoomServiceClient and method.name == "CloseRoom":
        request.room_id = _fixture(fix, "close_room_id", request.room_id)
    elif client_cls is SignalingServiceClient and method.name == "Signal":
        request.peer_id = _fixture(fix, "signal_peer_id", request.peer_id)
    elif client_cls is TenantServiceClient and method.name == "UpdateTenantConfig":
        request.type = "string"
    elif client_cls is DataBrokerClient:
        if method.name in {"Select", "SelectV2", "BatchSelect", "Delete"} and hasattr(request, "filter"):
            _set_struct(request.filter, {"record_id": _fixture(fix, "record_id", "python-live-record"), "tenant_id": meta.tenant_id, "project_id": meta.project_id})
        elif method.name == "Upsert":
            _set_struct(request.payload, {
                "record_id": f"py-perf-upsert-{suffix}",
                "tenant_id": meta.tenant_id,
                "project_id": meta.project_id,
                "lookup_key": f"py-perf-upsert-lk-{suffix}",
                "payload": "python-live",
            })
            request.conflict_fields[:] = ["record_id"]
        elif method.name in {"VectorSearch", "VectorHybridSearch", "VectorUpsert", "VectorBatchUpsert"}:
            request.collection = _fixture(fix, "vector_collection", "sdk_live_records")
        elif method.name in {"TimeSeriesQuery", "TimeSeriesWrite"} and hasattr(request, "resource"):
            request.resource.backend = "clickhouse"
            request.resource.resource_name = _fixture(fix, "ts_table", "sdk_perf_ts")
        elif method.name == "CreateMaterializedView":
            request.name = "mv_test"
        elif method.name == "ApplyMigration":
            request.run_id = _fixture(fix, "apply_run_id", request.run_id)
            request.approval_token = _fixture(fix, "approval_token", request.approval_token)
        elif method.name == "ApproveMigrationPlan":
            request.run_id = _fixture(fix, "approve_run_id", request.run_id)
        elif method.name == "GetMigrationStatus":
            request.run_id = _fixture(fix, "migration_id", request.run_id)
        elif method.name == "EnqueueOutboxEvent":
            _set_struct(request.payload, {
                "event_id": str(uuid.uuid4()),
                "event_type": _fixture(fix, "event_type", "python.live.perf"),
                "correlation_id": str(uuid.uuid4()),
                "document_id": _fixture(fix, "document_id", "python-live-document"),
            })
        elif method.name == "GenericDispatch":
            request.spec_json = "{}"
        elif method.name == "DropResource":
            request.spec_json = '{"udb_allow_rls_bypass":true}'
        elif method.name in {"StageCatalog", "ValidateCatalog"}:
            catalog_manifest = fix.lookup("catalog_manifest")
            if catalog_manifest:
                request.manifest_json = catalog_manifest.encode("utf-8")


def _fixture(fix, key: str, default: str = "") -> str:
    return fix.lookup(key) or default


def _doc_seed_key(field_name: str, body: str) -> str | None:
    # Match only the value assigned to this field. The previous loose "within
    # 80 chars" scan could steal a later field's seed tag, e.g. storage
    # content_type became <seed:file_id>.
    pattern = (
        rf"(?<![a-z0-9_])[`\"]?{re.escape(field_name.lower())}[`\"]?(?![a-z0-9_])"
        rf"\s*(?::|=)\s*`?\"?<seed:([^>]+)>"
    )
    match = re.search(pattern, body.lower())
    if match:
        return match.group(1)
    return None


def _doc_literal_string(field_name: str, body: str) -> str | None:
    pattern = (
        rf"(?<![a-z0-9_])[`\"]?{re.escape(field_name.lower())}[`\"]?(?![a-z0-9_])"
        rf"\s*(?::|=)\s*`?\"([^\"<`]*)\""
    )
    match = re.search(pattern, body, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _doc_explicit_empty(field_name: str, body: str) -> bool:
    pattern = (
        rf"(?<![a-z0-9_])[`\"]?{re.escape(field_name.lower())}[`\"]?(?![a-z0-9_])"
        rf"\s*(?::|=)\s*(?:\[\]|\{{\}})"
    )
    return re.search(pattern, body.lower()) is not None


def _doc_literal_bool(field_name: str, body: str) -> bool | None:
    pattern = (
        rf"(?<![a-z0-9_])[`\"]?{re.escape(field_name.lower())}[`\"]?(?![a-z0-9_])"
        rf"\s*(?::|=)\s*(true|false)"
    )
    match = re.search(pattern, body.lower())
    if match:
        return match.group(1) == "true"
    return None


def _doc_literal_int(field_name: str, body: str) -> int | None:
    pattern = (
        rf"(?<![a-z0-9_])[`\"]?{re.escape(field_name.lower())}[`\"]?(?![a-z0-9_])"
        rf"\s*(?::|=)\s*(-?\d+)"
    )
    match = re.search(pattern, body.lower())
    if match:
        return int(match.group(1))
    return None


def _field_seed(field_name: str, meta: Metadata, fix, body: str = "") -> str:
    tenant, project = meta.tenant_id, meta.project_id
    name = field_name.lower()
    doc_key = _doc_seed_key(name, body)
    if name in {"otp_id", "original_otp_id"} and doc_key == "code":
        doc_key = "otp_id"
    elif name == "challenge_id" and doc_key == "code":
        doc_key = "challenge_id"
    elif name == "device_id" and doc_key == "record_id":
        doc_key = "device_id"
    elif name == "dlq_id" and doc_key == "record_id":
        doc_key = "dlq_id"
    if name in {"created_by", "updated_by", "deleted_by", "assigned_by", "revoked_by", "approved_by", "rejected_by", "reviewer"}:
        return _fixture(fix, "user_id", "python-live-user")
    if doc_key:
        seeded = fix.lookup(doc_key)
        if seeded:
            return seeded
    literal = _doc_literal_string(name, body)
    if literal is not None:
        return literal
    direct = fix.lookup(name)
    if direct:
        return direct
    if name == "status":
        low = body.lower()
        if "completed" in low:
            return "COMPLETED"
        if "ready" in low:
            return "READY"
        if "pending" in low:
            return "PENDING"
        if "failed" in low:
            return "FAILED"
        return "active"
    if name == "state":
        low = body.lower()
        if "connected" in low:
            return "connected"
        if "active" in low:
            return "active"
        return "active"
    if name == "query":
        low = body.lower()
        if "match (n)" in low:
            return "MATCH (n) RETURN n LIMIT 1"
        if "create (n" in low:
            return "CREATE (n:Node {id:$id})"
        return "SELECT 1"
    aliases = {
        "tenant": tenant, "tenant_id": tenant, "domain": tenant,
        "project": project, "project_id": project,
        "message_type": LIVE_MESSAGE_TYPE,
        "username": _fixture(fix, "username", "sdk-live-perf@example.com"),
        "identifier": _fixture(fix, "identifier", _fixture(fix, "username", "sdk-live-perf@example.com")),
        "password": _fixture(fix, "password", "CorrectHorse1!"),
        "current_password": _fixture(fix, "password", "CorrectHorse1!"),
        "new_password": _fixture(fix, "new_password", "CorrectHorse2!"),
        "email": _fixture(fix, "email", "sdk-live-perf@example.com"),
        "full_name": "SDK Perf User",
        "reason": "python-live-perf",
        "revoke_reason": "python-live-perf",
        "rotation_reason": "python-live-perf",
        "change_reason": "python-live-perf",
        "device_name": "python-sdk-perf",
        "device_id": _fixture(fix, "device_id", ""),
        "device_fingerprint": "python-sdk-perf-device",
        "phone": "+15551234567",
        "otp_id": _fixture(fix, "otp_id", ""),
        "original_otp_id": _fixture(fix, "otp_id", ""),
        "challenge_id": _fixture(fix, "challenge_id", ""),
        "code": _fixture(fix, "otp_code", "123456"),
        "csrf_token": _fixture(fix, "csrf_token", "123456"),
        "token": _fixture(fix, "token", _fixture(fix, "access_token")),
        "bearer_token": _fixture(fix, "token", _fixture(fix, "access_token")),
        "refresh_token": _fixture(fix, "refresh_token"),
        "session_id": _fixture(fix, "session_id"),
        "user_id": _fixture(fix, "user_id", "python-live-user"),
        "principal_id": _fixture(fix, "user_id", "python-live-user"),
        "subject": _fixture(fix, "subject", f"user:{_fixture(fix, 'user_id', 'python-live-user')}"),
        "created_by": _fixture(fix, "user_id", "python-live-user"),
        "updated_by": _fixture(fix, "user_id", "python-live-user"),
        "deleted_by": _fixture(fix, "user_id", "python-live-user"),
        "assigned_by": _fixture(fix, "user_id", "python-live-user"),
        "revoked_by": _fixture(fix, "user_id", "python-live-user"),
        "approved_by": _fixture(fix, "user_id", "python-live-user"),
        "rejected_by": _fixture(fix, "user_id", "python-live-user"),
        "owner_id": _fixture(fix, "owner_id", _fixture(fix, "user_id", "python-live-user")),
        "role_id": _fixture(fix, "role_id", "python-live-role"),
        "role": _fixture(fix, "role", "sdk_reader"),
        "role_code": _fixture(fix, "role_code", "sdk_reader"),
        "user_role_id": _fixture(fix, "user_role_id", "python-live-user-role"),
        "policy_id": _fixture(fix, "policy_id", "1"),
        "policy_draft_id": _fixture(fix, "policy_draft_id", _fixture(fix, "policy_id", "python-live-policy")),
        "policy_set_id": _fixture(fix, "policy_set_id", _fixture(fix, "policy_id", "python-live-policy")),
        "policy_version_id": _fixture(fix, "policy_version_id", _fixture(fix, "policy_id", "python-live-policy")),
        "target_version_id": _fixture(fix, "target_version_id", _fixture(fix, "policy_id", "python-live-policy")),
        "against_version_id": "",
        "canary_id": _fixture(fix, "canary_id", _fixture(fix, "policy_id", "python-live-policy")),
        "draft_id": _fixture(fix, "policy_draft_id", _fixture(fix, "policy_id", "python-live-policy")),
        "id": _fixture(fix, doc_key or "record_id", "python-live-record"),
        "credential_id": _fixture(fix, "record_id", "python-live-credential"),
        "key_id": _fixture(fix, "key_id", "python-live-key"),
        "key_prefix": "",
        "plain_key": _fixture(fix, "plain_key", "udbk_python_live_key"),
        "ip_allowlist": "127.0.0.1/32",
        "stage_name": _fixture(fix, "stage_name", "python_live_stage"),
        "executor_identity": "python-live-executor",
        "operation_name": "python-live-operation",
        "workload_kind": "pipeline",
        "hour": "2026-06-14T00:00:00Z",
        "hour_from": "2026-06-14T00:00:00Z",
        "hour_to": "2026-06-14T23:00:00Z",
        "date_from": "2026-06-01",
        "date_to": "2026-06-14",
        "event_type": _fixture(fix, "event_type", "python.live.perf"),
        "log_id": _fixture(fix, "log_id", "python-live-log"),
        "notification_id": _fixture(fix, "notification_id", "python-live-log"),
        "template_id": _fixture(fix, "template_id", _fixture(fix, "event_type", "python.live.perf")),
        "file_id": _fixture(fix, "file_id", "python-live-file"),
        "definition_id": _fixture(fix, "definition_id", "python-live-definition"),
        "asset_id": _fixture(fix, "asset_id", "python-live-asset"),
        "instance_id": _fixture(fix, "instance_id", "python-live-instance"),
        "room_id": _fixture(fix, "room_id", "python-live-room"),
        "peer_id": _fixture(fix, "peer_id", "python-live-peer"),
        "track_id": _fixture(fix, "track_id", "python-live-track"),
        "provider_id": _fixture(fix, "provider_id", "python-live-provider"),
        "external_identity_id": _fixture(fix, "external_identity_id", "python-live-external-identity"),
        "scim_user_id": _fixture(fix, "scim_user_id", "python-live-scim-user"),
        "scim_group_id": _fixture(fix, "scim_group_id", "python-live-scim-group"),
        "migration_id": _fixture(fix, "migration_id", "python-live-migration"),
        "run_id": _fixture(fix, "migration_id", "python-live-migration"),
        "saga_id": _fixture(fix, "saga_id", "python-live-saga"),
        "dlq_id": _fixture(fix, "dlq_id", _fixture(fix, "record_id", "python-live-dlq")),
        "bucket": _fixture(fix, "bucket", "udb-live-sdk"),
        "object_key": _fixture(fix, "object_key", "python-live-object.txt"),
        "document_id": _fixture(fix, "document_id", "python-live-document"),
        "collection": _fixture(fix, "collection", LIVE_MESSAGE_TYPE),
        "vector_collection": _fixture(fix, "vector_collection", "sdk_live_records"),
        "mongo_collection": _fixture(fix, "mongo_collection", "sdk_perf_docs"),
        "resource_name": _fixture(fix, "mongo_collection", "sdk_perf_docs"),
        "resource": _fixture(fix, "resource", "invoice"),
        "resource_type": _fixture(fix, "resource", "invoice"),
        "effect": "allow",
        "object": _fixture(fix, "object", "group:python-live"),
        "action": _fixture(fix, "action", "data.select"),
        "relation": _fixture(fix, "relation", "member"),
        "scope": "udb:admin",
        "required_scope": "udb:read",
        "scope_values": "10",
        "spec_json": "{}",
        "backend": "mongodb",
        "operation": "ping",
        "schema": "public",
        "query": "SELECT 1",
        "table": LIVE_MESSAGE_TYPE,
        "text_query": "hello",
        "key": _fixture(fix, "object_key", "python-live-object.txt"),
        "key_pattern": "*",
        "conflict_fields": "record_id",
        "version": "",
        "name": _fixture(fix, "name", "python-live-perf"),
        "display_name": "Python Live Perf",
        "description": "python-live-perf",
        "label": "python-live",
        "title": "python-live",
        "source": "manual",
        "locale": "en",
        "channel": str(NOTIFICATION_CHANNEL_EMAIL),
        "recipient_id": _fixture(fix, "user_id", "python-live-user"),
        "recipient_address": "sdk-live-perf@example.com",
        "subject_template": "Hello {{name}}",
        "body_template": "python-live-body",
        "steps": '[{"name":"extract","type":"EXTRACT"}]',
        "error_message": "",
        "new_label": "work key",
        "filename": _fixture(fix, "filename", "python-live.txt"),
        "content_type": "text/plain",
        "file_type": STORAGE_FILE_TYPE,
        "media_type": "application/json",
        "reference_id": _fixture(fix, "record_id", "python-live-record"),
        "reference_type": "sdk.perf",
        "kind": "audio",
        "state": "active",
        "status": "active",
        "method": "GET",
        "endpoint": "/v1/test",
        "topic": _fixture(fix, "event_type", "python.live.perf"),
        "topic_pattern": "*",
        "partition_key": _fixture(fix, "document_id", "python-live-document"),
        "slot_name": "udb_cdc",
        "redaction_mode": "mask",
        "scan_mode": "sample",
        "cdc_topic_prefix": f"{project}.",
        "policy_set_name": "default",
        "reviewer": _fixture(fix, "user_id", "python-live-reviewer"),
        "node_id": "python-live-node",
        "response_nonce": "",
        "filter": "",
        "op": "replace",
        "path": "active",
        "value_json": "false",
        "step_id": _fixture(fix, "step_id", "python-live-step"),
        "correlation_id": str(uuid.uuid4()),
        "ip_address": "127.0.0.1",
        "user_agent": "python-sdk-perf",
        "uploaded_by": _fixture(fix, "user_id", "python-live-user"),
        "parent_tenant_id": "",
        "branding": "{}",
        "config_key": "feature.flag",
        "config_value": "on",
        "type": "string" if "config_key" in body.lower() else "organization",
        "issuer": "https://idp.example.com",
        "jwks_url": "https://idp.example.com/jwks",
        "client_id": "client-1",
        "audience": "udb",
        "relay_state": "state-1",
        "saml_response": "PHNhbWxwOlJlc3BvbnNlLz4=",
        "metadata_xml": SAML_IDP_METADATA_XML,
        "claims_json": '{"sub":"abc","email":"a@x.com","email_verified":true}',
        "claim_mapping_json": "{}",
        "group_mapping_json": "{}",
        "jit_policy_json": "{}",
        "account_linking_policy": "explicit",
        "scim_user_json": '{"userName":"a@x.com","active":true}',
        "scim_group_json": '{"displayName":"sdk-perf-group"}',
        "public_key_credential_json": '{"id":"bench","rawId":"bench","type":"public-key","response":{}}',
        "metadata": "{}",
        "settings": "{}",
        "config": "{}",
        "context": "{}",
        "result": "{}",
    }
    if name in aliases:
        return aliases[name]
    if literal is not None:
        return literal
    raise MissingExplicitPerfBody(f"no explicit doc-backed value for string field {field_name!r}")


def _enum_number(field, body: str) -> int:
    if field.enum_type is None:
        return 0
    for value in field.enum_type.values:
        if value.name != value.name.upper():
            continue
        if value.name != "UNSPECIFIED" and value.name in body:
            return value.number
    for value in field.enum_type.values:
        if value.number != 0:
            return value.number
    return field.enum_type.values[0].number if field.enum_type.values else 0


def _set_struct(msg, values: dict) -> None:
    msg.Clear()
    msg.update(values)


def _fill_common_context(msg, meta: Metadata) -> None:
    if hasattr(msg, "tenant"):
        msg.tenant.tenant_id = meta.tenant_id
        msg.tenant.project_id = meta.project_id
    if hasattr(msg, "purpose"):
        msg.purpose = "python.live.perf"
    if hasattr(msg, "user_agent"):
        msg.user_agent = "python-sdk-perf"


def _fill_entity_context(msg, meta: Metadata) -> None:
    msg.CopyFrom(meta.to_request_context())
    if hasattr(msg, "purpose"):
        msg.purpose = "python.live.perf"


def _fill_message(msg, field_name: str, body: str, meta: Metadata, fix) -> None:
    full = msg.DESCRIPTOR.full_name
    if full == "google.protobuf.Struct":
        vals = {"tenant_id": meta.tenant_id, "project_id": meta.project_id}
        if field_name == "filter":
            vals.update({"record_id": _fixture(fix, "record_id", "python-live-record")})
        elif field_name == "parameters":
            vals.update({"record_id": _fixture(fix, "record_id", "python-live-record"), "id": _fixture(fix, "record_id", "python-live-record")})
        elif field_name in {"payload", "document", "fields"}:
            if "event_id" in body:
                vals.update({
                    "event_id": str(uuid.uuid4()),
                    "event_type": _fixture(fix, "event_type", "python.live.perf"),
                    "correlation_id": str(uuid.uuid4()),
                    "document_id": _fixture(fix, "document_id", "python-live-document"),
                })
            else:
                vals.update({
                    "record_id": _fixture(fix, "record_id", "python-live-record"),
                    "lookup_key": f"py-perf-lk-{uuid.uuid4().hex[:12]}",
                    "payload": "python-live",
                })
        elif field_name == "metadata":
            vals = {"source": "python-live-perf"}
        _set_struct(msg, vals)
        return
    if full == "google.protobuf.Timestamp":
        msg.FromSeconds(int(time.time()))
        return
    if full == "udb.core.common.v1.RequestContext":
        _fill_common_context(msg, meta)
        return
    if full == "udb.entity.v1.RequestContext":
        _fill_entity_context(msg, meta)
        return
    if full == "udb.core.authz.services.v1.GovernanceActor":
        # The live D1/D2 governance gate evaluates scopes from the VERIFIED claim,
        # NOT request-body actor.scopes, and no role projects to authz:*
        # (tokens.rs ROLE_SCOPE_PROJECTIONS). So body scopes can never satisfy the
        # gate here; use the body-authoritative break-glass bypass instead (≤900s,
        # reason-bearing, audited). gov_exp is seeded to now+900 in perf_seed.
        msg.subject = _fixture(fix, "subject", f"user:{_fixture(fix, 'user_id', 'python-live-user')}")
        msg.tenant_id = meta.tenant_id
        msg.project_id = meta.project_id
        msg.break_glass = True
        msg.break_glass_reason = "sdk perf bench"
        msg.break_glass_expires_at_unix = int(_fixture(fix, "gov_exp", str(int(time.time()) + 900)))
        return
    if full.endswith(".StoreResource"):
        low = body.lower()
        backend = ""
        for candidate in ("redis", "mongodb", "neo4j", "clickhouse", "minio", "qdrant"):
            if f'backend:"{candidate}"' in low or f'backend="{candidate}"' in low or f'backend:`"{candidate}"' in low or candidate in low:
                backend = candidate
                break
        msg.backend = backend or "mongodb"
        if msg.backend == "mongodb":
            msg.resource_name = _fixture(fix, "mongo_collection", "sdk_perf_docs")
        elif msg.backend == "minio":
            msg.resource_name = _fixture(fix, "bucket", "udb-live-sdk")
        elif msg.backend == "clickhouse":
            msg.resource_name = _fixture(fix, "ts_table", "sdk_perf_ts")
        elif msg.backend == "qdrant":
            msg.resource_name = "sdk_live_records"
        if hasattr(msg, "message_type"):
            msg.message_type = _fixture(fix, "message_type", LIVE_MESSAGE_TYPE)
        return
    if full.endswith(".PageRequest"):
        if hasattr(msg, "page"):
            msg.page = 1
        if hasattr(msg, "page_size"):
            msg.page_size = 50
        return
    for field in msg.DESCRIPTOR.fields:
        if doc_mentions_field(field, body):
            set_doc_field(msg, field, body, meta, fix)


def _scope_values_for_body(body: str, field_name: str) -> list[str]:
    quoted = re.findall(r'"([^"]+)"', body)
    scopes = [v[4:] if v.startswith("udb:authz:") else v for v in quoted if "authz:" in v or v.startswith("udb:")]
    if scopes:
        return scopes
    if field_name == "required_scopes":
        return ["udb:read"]
    if "policy" in body.lower() or "governance" in body.lower():
        return ["authz:policy:write"]
    return ["udb:admin"]


def _append_doc_value(container, field, body: str, meta: Metadata, fix) -> None:
    if _doc_explicit_empty(field.name, body):
        return
    if field.message_type is not None and field.message_type.GetOptions().map_entry:
        value_field = field.message_type.fields_by_name["value"]
        if value_field.type == _FD.TYPE_STRING:
            container["python-live"] = "true"
        elif value_field.type in INT_FD_TYPES:
            container["python-live"] = 1
        elif value_field.type in (_FD.TYPE_DOUBLE, _FD.TYPE_FLOAT):
            container["python-live"] = 1.0
        elif value_field.type == _FD.TYPE_BOOL:
            container["python-live"] = True
        return
    if field.type == _FD.TYPE_STRING:
        if field.name in {"scopes", "requested_scopes", "required_scopes"}:
            container.extend(_scope_values_for_body(body, field.name))
        elif field.name in {"channels"}:
            container.append(str(NOTIFICATION_CHANNEL_EMAIL))
        elif field.name in {"client_ids"}:
            container.append("client-1")
        elif field.name in {"audiences"}:
            container.append("udb")
        elif field.name in {"groups"}:
            container.append("admins")
        elif field.name in {"resource_names", "resource_names_subscribe"}:
            container.append(_fixture(fix, "resource_name", "python-live-resource"))
        elif field.name in {"fusion_weights"}:
            container.append("0.5")
        else:
            container.append(_field_seed(field.name, meta, fix, body))
    elif field.type in INT_FD_TYPES:
        container.append(1)
    elif field.type in (_FD.TYPE_DOUBLE, _FD.TYPE_FLOAT):
        if field.name == "vector":
            container.extend([0.1, 0.2, 0.3])
        else:
            container.append(0.1)
    elif field.type == _FD.TYPE_BOOL:
        container.append(True)
    elif field.type == _FD.TYPE_ENUM:
        container.append(_enum_number(field, body))
    elif field.type == _FD.TYPE_MESSAGE:
        item = container.add()
        _fill_message(item, field.name, body, meta, fix)


def set_doc_field(msg, field, body: str, meta: Metadata, fix) -> None:
    if field.label == _FD.LABEL_REPEATED:
        _append_doc_value(getattr(msg, field.name), field, body, meta, fix)
        return
    if field.type == _FD.TYPE_STRING:
        setattr(msg, field.name, _field_seed(field.name, meta, fix, body))
    elif field.type == _FD.TYPE_BYTES:
        if field.name == "record_json":
            setattr(msg, field.name, live_record_json(_fixture(fix, "record_id", "python-live-record"), meta.tenant_id, meta.project_id, "py-perf-lk", "python-live", 1))
        elif field.name == "payload_json":
            setattr(msg, field.name, json.dumps({"event_id": str(uuid.uuid4()), "event_type": _fixture(fix, "event_type", "python.live.perf"), "document_id": _fixture(fix, "document_id", "python-live-document")}).encode())
        elif field.name == "manifest_json":
            catalog_manifest = fix.lookup("catalog_manifest")
            setattr(msg, field.name, catalog_manifest.encode("utf-8") if catalog_manifest else b'{"checksum_sha256":"python-live-perf","schemas":[]}')
        else:
            setattr(msg, field.name, b"python-live")
    elif field.type == _FD.TYPE_BOOL:
        literal = _doc_literal_bool(field.name, body)
        if literal is not None:
            setattr(msg, field.name, literal)
        else:
            setattr(msg, field.name, field.name not in {"dry_run", "redact", "repair", "preserve_event_id", "all_sessions", "all_for_principal", "only_if_absent", "only_if_present"})
    elif field.type in INT_FD_TYPES:
        doc_key = _doc_seed_key(field.name, body)
        seeded = fix.lookup(doc_key) if doc_key else None
        literal = _doc_literal_int(field.name, body)
        if seeded:
            setattr(msg, field.name, int(seeded))
        elif literal is not None:
            setattr(msg, field.name, literal)
        elif field.name in {"limit", "page_size", "ttl_seconds", "expires_in_minutes", "count", "rows_per_target", "rate_limit_per_minute", "rate_limit_per_day", "success_window_secs", "min_samples"}:
            setattr(msg, field.name, 10)
        elif field.name in {"expected_revision", "expected_policy_revision", "expected_relationship_revision", "expected_updated_at_unix"}:
            # Optimistic-concurrency tokens: 0 == "skip the check" (governance_activate.rs:47).
            # Go omits them entirely; filling 1 trips "revision changed concurrently" on
            # ActivatePolicyVersion once the live revision has advanced past 1.
            setattr(msg, field.name, 0)
        elif field.name in {"part_count", "version", "redaction_version", "priority"}:
            setattr(msg, field.name, 1)
        elif "size" in field.name:
            setattr(msg, field.name, 128)
        else:
            setattr(msg, field.name, 1)
    elif field.type in (_FD.TYPE_DOUBLE, _FD.TYPE_FLOAT):
        setattr(msg, field.name, 0.99 if "threshold" in field.name else 100.0)
    elif field.type == _FD.TYPE_ENUM:
        setattr(msg, field.name, _enum_number(field, body))
    elif field.type == _FD.TYPE_MESSAGE:
        _fill_message(getattr(msg, field.name), field.name, body, meta, fix)


def apply_doc_fields(request, field_names: set[str], body: str, meta: Metadata, fix) -> None:
    for field in request.DESCRIPTOR.fields:
        if field.name in field_names:
            set_doc_field(request, field, body, meta, fix)


# --------------------------------------------------------------------------------
# Perf SEED phase + fixture map (Python counterpart of the Go harness in
# live_perf_seed_test.go / live_surface_probe_test.go). The perf run measures REAL
# successful-call latency for the RPC surface; every measured RPC must use an
# explicit, doc-backed request body from perf_real_body. Missing bodies fail fast
# as MissingExplicitPerfBody instead of falling back to reflective filler.
# --------------------------------------------------------------------------------


def put_presigned_storage_object(upload_url: str, data: bytes, content_type: str) -> None:
    """PUT bytes to the StorageService-minted presigned upload_url (harness_correction.md:
    FinalizeUpload). The native service owns its object bucket/instance, so the bytes MUST
    land via the service-minted URL, not the catalog-gated DataBroker PutObject path which
    may write a different object-plane target than FinalizeUpload later HEADs."""
    import urllib.request

    if not (upload_url or "").strip():
        raise ValueError("empty storage upload_url")
    req = urllib.request.Request(upload_url, data=data, method="PUT")
    if content_type:
        req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=10) as resp:
        status = getattr(resp, "status", resp.getcode())
        if status < 200 or status >= 300:
            raise RuntimeError(f"presigned storage PUT failed: {status}")


class PerfFixtures:
    """Maps a semantic field name to a real seeded value."""

    def __init__(self) -> None:
        self.m: dict[str, str] = {}

    def set(self, key: str, val: str) -> None:
        if val:
            self.m[key.lower()] = val

    def lookup(self, field: str) -> str | None:
        if field in self.m:
            return self.m[field]
        for k, v in self.m.items():
            if field == k or field.endswith("_" + k):
                return v
        return None

def perf_seed(clients: dict, meta: Metadata):
    """Create real, disposable entities across the services the perf run touches and
    record their identifiers. Mirrors the Go ``perfSeed``: seeds in DEPENDENCY ORDER
    (a user before a role assignment before a notification; a file before an asset;
    a room before a peer before a track), namespaces everything by a per-run suffix,
    and returns ``(fixtures, record_id, cleanup)``. ``meta.tenant_id`` is the canonical
    tenant UUID discovered from the principal, so the UUID-strict native services
    (storage/asset/webrtc) and the free-text services share one bearer (auth_fix.md)."""
    fix = PerfFixtures()
    suffix = uuid.uuid4().hex
    tenant, project = meta.tenant_id, meta.project_id
    md = meta.to_grpc_metadata()
    cleanups: list = []

    broker = clients[DataBrokerClient].stub
    rc = meta.with_purpose("python.live.perf.seed").to_request_context()

    # Always-known scalars.
    fix.set("tenant_id", tenant)
    fix.set("tenant", tenant)
    fix.set("project_id", project)
    fix.set("project", project)
    fix.set("domain", tenant)
    fix.set("message_type", LIVE_MESSAGE_TYPE)
    fix.set("locale", "en")
    fix.set("name", f"sdk-perf-{suffix}")
    fix.set("filename", f"sdk-perf-{suffix}.txt")
    fix.set("content_type", "text/plain")
    fix.set("file_type", STORAGE_FILE_TYPE)
    fix.set("kind", "audio")
    fix.set("topic_pattern", "*")
    fix.set("migration_id", str(uuid.uuid4()))
    fix.set("egress_id", f"eg-{tenant}-{suffix}")
    fix.set("purge_tenant_id", tenant)

    # Recovery fixtures come from the served, admin-gated DataBroker path, not
    # raw udb_system inserts. Each mutating RPC gets a disposable row.
    for saga_key, dlq_key in (
        ("saga_id", "dlq_id"),
        ("retry_saga_id", "dismiss_dlq_id"),
        ("mark_saga_id", "quarantine_dlq_id"),
        ("", "replay_dlq_id"),
    ):
        try:
            baseline = broker.EnsureBaseline(
                data_broker_pb2.EnsureBaselineRequest(context=rc),
                metadata=md,
                timeout=8.0,
            )
            if saga_key and baseline.saga_ids:
                fix.set(saga_key, baseline.saga_ids[0])
            if baseline.dlq_ids:
                fix.set(dlq_key, baseline.dlq_ids[0])
        except grpc.RpcError:
            break

    # ── DataBroker: a real SdkLiveRecord row (drives Upsert/Select/Delete + CDC) ──
    record_id = f"py-perf-{suffix}"
    try:
        broker.Upsert(
            relational_pb2.UpsertRequest(
                context=rc, message_type=LIVE_MESSAGE_TYPE,
                record_json=live_record_json(record_id, tenant, project, f"py-perf-lk-{suffix}", "perf-seed", 1),
                conflict_fields=["record_id"],
            ),
            metadata=md, timeout=8.0,
        )
    except grpc.RpcError:
        pass
    fix.set("record_id", record_id)

    proj_id = f"sdklive_perf_{suffix}"
    try:
        broker.EnsureProject(admin_pb2.EnsureProjectRequest(context=rc, project_id=proj_id, name="SDK Perf Project"), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass

    # A real MinIO bucket + object so GetObject and the object RPCs run their success path.
    bucket = os.getenv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
    object_key = f"py-perf/{suffix}.txt"
    try:
        broker.EnsureResource(admin_pb2.ResourceAdminRequest(context=rc, backend="minio", resource_name=bucket, spec_json="{}"), metadata=md, timeout=8.0)
        broker.PutObject(
            iter([blob_pb2.Chunk(context=rc, bucket=bucket, object_key=object_key, data=f"py-perf-object-{suffix}".encode(), content_type="text/plain", final_chunk=True)]),
            metadata=md, timeout=8.0,
        )
    except grpc.RpcError:
        pass
    fix.set("bucket", bucket)
    fix.set("object_key", object_key)

    # A real Mongo collection + document so the document RPCs resolve a resource.
    collection = f"sdk_perf_docs_{suffix}"
    document_id = f"doc-perf-{suffix}"
    try:
        broker.EnsureResource(admin_pb2.ResourceAdminRequest(context=rc, backend="mongodb", resource_name=collection, spec_json=json.dumps({"collection": collection})), metadata=md, timeout=8.0)
        broker.DocumentUpsert(
            stores_pb2.DocumentUpsertRequest(
                context=rc, resource=operation_pb2.StoreResource(backend="mongodb", resource_name=collection),
                document_id=document_id, document=live_struct({"_id": document_id, "payload": "perf", "revision": 1}),
            ),
            metadata=md, timeout=8.0,
        )
    except grpc.RpcError:
        pass
    # NOTE: a single backend/resource_name fixture cannot serve both the SQL and the
    # document/cache/vector/graph RPCs, so those backend-specific DataBroker RPCs are
    # driven by typed bodies in perf_real_body. We deliberately do NOT register a
    # global backend/resource_name fixture.
    fix.set("collection", collection)
    fix.set("mongo_collection", collection)
    fix.set("document_id", document_id)
    try:
        broker.EnsureResource(
            admin_pb2.ResourceAdminRequest(context=rc, backend="qdrant", resource_name="sdk_live_records", spec_json='{"size":3,"distance":"Cosine"}'),
            metadata=md, timeout=8.0,
        )
    except grpc.RpcError:
        pass
    try:
        broker.EnsureResource(
            admin_pb2.ResourceAdminRequest(context=rc, backend="clickhouse", resource_name="sdk_perf_ts", spec_json="{}"),
            metadata=md, timeout=8.0,
        )
    except grpc.RpcError:
        pass
    fix.set("vector_collection", "sdk_live_records")
    fix.set("ts_table", "sdk_perf_ts")

    # ── AuthnService: a real user (id reused everywhere a user_id is needed) ──────
    authn = clients[AuthnServiceClient].stub
    pw = "CorrectHorse1!"
    uname = f"sdk-perf-{suffix}"
    email = f"{uname}@example.com"
    fix.set("username", uname)
    fix.set("identifier", email)
    fix.set("email", email)
    fix.set("password", pw)
    fix.set("current_password", pw)
    fix.set("new_password", "CorrectHorse2!")
    uid = ""
    try:
        created = authn.CreateUser(
            authn_pb2.CreateUserRequest(username=uname, email=email, password=pw, tenant_id=tenant, project_id=project, full_name="SDK Perf User"),
            metadata=md, timeout=8.0,
        )
        uid = created.user.user_id
    except grpc.RpcError:
        pass
    if uid:
        for key in ("user_id", "recipient_id", "assigned_by", "created_by", "updated_by", "revoked_by", "deleted_by", "approved_by", "rejected_by", "owner_id"):
            fix.set(key, uid)
        fix.set("subject", f"user:{uid}")
        try:
            authn.ChangeUserStatus(
                authn_pb2.ChangeUserStatusRequest(
                    user_id=uid,
                    new_status=2,
                    reason="perf seed activate",
                    context=common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project)),
                ),
                metadata=md, timeout=8.0,
            )
        except grpc.RpcError:
            pass
        try:
            login = authn.Login(
                authn_pb2.LoginRequest(
                    username=uname,
                    password=pw,
                    tenant_hint=tenant,
                    project_hint=project,
                    device_name="python-sdk-perf-seed",
                    device_id=f"python-sdk-perf-device-{suffix}",
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("session_id", login.session_id)
            fix.set("token", login.access_token)
            fix.set("access_token", login.access_token)
            fix.set("refresh_token", login.refresh_token)
            fix.set("csrf_token", login.csrf_token)
        except grpc.RpcError:
            pass
        try:
            codes = authn.GenerateRecoveryCodes(authn_pb2.GenerateRecoveryCodesRequest(user_id=uid, count=8), metadata=md, timeout=8.0)
            if codes.codes:
                fix.set("code", codes.codes[0])
                fix.set("recovery_code", codes.codes[0])
        except (grpc.RpcError, AttributeError):
            pass
        try:
            devices = authn.ListDevices(authn_pb2.ListDevicesRequest(user_id=uid), metadata=md, timeout=8.0)
            if devices.devices:
                fix.set("device_id", devices.devices[0].device_id)
        except (grpc.RpcError, AttributeError):
            pass
        try:
            challenge = authn.IssueMfaChallenge(
                authn_pb2.IssueMfaChallengeRequest(
                    user_id=uid,
                    factor_kind=2,
                    purpose=1,
                    device_fingerprint=f"python-sdk-perf-{suffix}",
                    ip_address="127.0.0.1",
                    context=common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project)),
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("challenge_id", challenge.challenge_id)
        except (grpc.RpcError, AttributeError):
            pass
        # WebAuthn dev soft-authenticator (broker UDB_WEBAUTHN_TEST_MODE=1): register a real
        # passkey (Start->Finish with the sentinel) so StartWebAuthnAuthentication has one.
        # The dev authenticator is deterministic (one credential id per user), so the
        # measured FinishWebAuthnRegistration gets a challenge for a separate throwaway
        # user with no existing passkey; otherwise it measures duplicate/exclude handling.
        try:
            sr = authn.StartWebAuthnRegistration(
                authn_pb2.StartWebAuthnRegistrationRequest(user_id=uid, label="perf-passkey", tenant_id=tenant, project_id=project),
                metadata=md, timeout=8.0,
            )
            authn.FinishWebAuthnRegistration(
                authn_pb2.FinishWebAuthnRegistrationRequest(challenge_id=sr.challenge_id, public_key_credential_json=WEBAUTHN_TEST_CREDENTIAL, label="perf-passkey"),
                metadata=md, timeout=8.0,
            )
        except (grpc.RpcError, AttributeError):
            pass
        reg_user_id = ""
        try:
            reg_user = authn.CreateUser(
                authn_pb2.CreateUserRequest(
                    username=f"sdk-perf-webauthn-reg-{suffix}",
                    email=f"sdk-perf-webauthn-reg-{suffix}@example.com",
                    password=pw,
                    tenant_id=tenant,
                    project_id=project,
                    full_name="SDK Perf WebAuthn Registration User",
                ),
                metadata=md, timeout=8.0,
            )
            reg_user_id = reg_user.user.user_id
        except (grpc.RpcError, AttributeError):
            reg_user_id = uid
        try:
            sr2 = authn.StartWebAuthnRegistration(
                authn_pb2.StartWebAuthnRegistrationRequest(user_id=reg_user_id or uid, label="perf-passkey-2", tenant_id=tenant, project_id=project),
                metadata=md, timeout=8.0,
            )
            fix.set("reg_challenge_id", sr2.challenge_id)
        except (grpc.RpcError, AttributeError):
            pass
        try:
            sa = authn.StartWebAuthnAuthentication(
                authn_pb2.StartWebAuthnAuthenticationRequest(user_id=uid, tenant_id=tenant, project_id=project),
                metadata=md, timeout=8.0,
            )
            fix.set("auth_challenge_id", sa.challenge_id)
        except (grpc.RpcError, AttributeError):
            pass
        try:
            otp_user = authn.CreateUser(
                authn_pb2.CreateUserRequest(
                    username=f"sdk-perf-otp-{suffix}",
                    email=f"sdk-perf-otp-{suffix}@example.com",
                    password=pw,
                    tenant_id=tenant,
                    project_id=project,
                    full_name="SDK Perf OTP User",
                ),
                metadata=md, timeout=8.0,
            )
            otp = authn.SendOTP(
                authn_pb2.SendOTPRequest(
                    user_id=otp_user.user.user_id,
                    otp_type=4,
                    context=common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project)),
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("otp_id", otp.otp_id)
            fix.set("otp_code", otp.dev_otp_code)
            reset = authn.SendOTP(
                authn_pb2.SendOTPRequest(
                    user_id=otp_user.user.user_id,
                    otp_type=3,
                    context=common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project)),
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("reset_otp_id", reset.otp_id)
            fix.set("reset_otp_code", reset.dev_otp_code)
        except (grpc.RpcError, AttributeError):
            pass
        try:
            send_otp_user = authn.CreateUser(
                authn_pb2.CreateUserRequest(
                    username=f"sdk-perf-send-otp-{suffix}",
                    email=f"sdk-perf-send-otp-{suffix}@example.com",
                    password=pw,
                    tenant_id=tenant,
                    project_id=project,
                    full_name="SDK Perf Send OTP User",
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("send_otp_user_id", send_otp_user.user.user_id)
        except (grpc.RpcError, AttributeError):
            pass
        for seed_key, username_prefix in (
            ("change_status_user_id", "status"),
            ("admin_reset_password_user_id", "admin-rpw"),
            ("disable_mfa_user_id", "disable-mfa"),
            ("revoke_recovery_user_id", "revoke-recovery"),
            ("admin_reset_mfa_user_id", "admin-mfa"),
        ):
            try:
                disposable = authn.CreateUser(
                    authn_pb2.CreateUserRequest(
                        username=f"sdk-perf-{username_prefix}-{suffix}",
                        email=f"sdk-perf-{username_prefix}-{suffix}@example.com",
                        password=pw,
                        tenant_id=tenant,
                        project_id=project,
                        full_name=f"SDK Perf {username_prefix} User",
                    ),
                    metadata=md, timeout=8.0,
                )
                disposable_id = disposable.user.user_id
                fix.set(seed_key, disposable_id)
                if seed_key == "revoke_recovery_user_id":
                    try:
                        authn.GenerateRecoveryCodes(
                            authn_pb2.GenerateRecoveryCodesRequest(user_id=disposable_id, count=4),
                            metadata=md, timeout=8.0,
                        )
                    except grpc.RpcError:
                        pass
            except (grpc.RpcError, AttributeError):
                pass
        try:
            revoke_device_user = authn.CreateUser(
                authn_pb2.CreateUserRequest(
                    username=f"sdk-perf-revoke-device-{suffix}",
                    email=f"sdk-perf-revoke-device-{suffix}@example.com",
                    password=pw,
                    tenant_id=tenant,
                    project_id=project,
                    full_name="SDK Perf Revoke Device User",
                ),
                metadata=md, timeout=8.0,
            ).user
            authn.Login(
                authn_pb2.LoginRequest(
                    username=revoke_device_user.username,
                    password=pw,
                    tenant_hint=tenant,
                    project_hint=project,
                    device_name="python-sdk-revoke-device",
                    device_id=f"python-sdk-revoke-device-{suffix}",
                ),
                metadata=md, timeout=8.0,
            )
            devices = authn.ListDevices(authn_pb2.ListDevicesRequest(user_id=revoke_device_user.user_id), metadata=md, timeout=8.0)
            if devices.devices:
                fix.set("revoke_device_id", devices.devices[0].device_id)
        except (grpc.RpcError, AttributeError):
            pass

    # ── AuthzService: role + assignment + policies + relationship ─────────────────
    authz = clients[AuthzServiceClient].stub
    role_code = f"sdk_perf_reader_{suffix}"
    try:
        role = authz.CreateRole(
            authz_pb.CreateRoleRequest(name=f"SDK Perf Reader {suffix}", description="perf seed role", created_by=uid or str(uuid.uuid4()), role_code=role_code, domain=tenant, tenant_id=tenant, project_id=project),
            metadata=md, timeout=8.0,
        ).role
        rid = role.role_id
        fix.set("role_id", rid)
        fix.set("role", role_code)
        fix.set("role_code", role_code)
        if uid:
            try:
                assigned = authz.AssignRole(authz_pb.AssignRoleRequest(user_id=uid, role_id=rid, domain=tenant, assigned_by=uid, tenant_id=tenant, project_id=project), metadata=md, timeout=8.0).user_role
                fix.set("user_role_id", assigned.user_role_id)
            except grpc.RpcError:
                pass
        cleanups.append(lambda: authz.DeleteRole(authz_pb.DeleteRoleRequest(role_id=rid, deleted_by=uid), metadata=md, timeout=8.0))
    except grpc.RpcError:
        pass
    if uid:
        extra_role_code = f"sdk_perf_target_{suffix}"
        try:
            extra = authz.CreateRole(
                authz_pb.CreateRoleRequest(
                    name=f"SDK Perf Target {suffix}",
                    description="perf seed target role",
                    created_by=uid,
                    role_code=extra_role_code,
                    domain=tenant,
                    tenant_id=tenant,
                    project_id=project,
                ),
                metadata=md, timeout=8.0,
            ).role
            fix.set("role_id", extra.role_id)
            fix.set("role", extra_role_code)
            fix.set("role_code", extra_role_code)
            cleanups.append(lambda: authz.DeleteRole(authz_pb.DeleteRoleRequest(role_id=extra.role_id, deleted_by=uid), metadata=md, timeout=8.0))
        except grpc.RpcError:
            pass
    # ABAC policy + an RBAC policy rule. Capture policy_id only after the exact
    # GetPolicyRule path accepts it; a tenant-wide ListPolicyRules row can be an
    # older unrelated policy in dirty live benches.
    try:
        authz.PutAuthzPolicy(
            authz_pb.PutAuthzPolicyRequest(policy=authz_pb.AuthzPolicyRecord(id=str(uuid.uuid4()), enabled=True, effect="allow", tenant=tenant, project=project, role=role_code, action="data.select", resource="invoice")),
            metadata=md, timeout=8.0,
        )
    except grpc.RpcError:
        pass
    if uid:
        # ActivatePolicyVersion/RollbackPolicyVersion DELETE policy_rules WHERE tenant=$1 AND
        # project=$2 for the activated (main) project and re-insert the version's rules with
        # FRESH gen_random_uuid() ids (governance_activate.rs:236,274). Those measured RPCs sort
        # BEFORE GetPolicyRule, so a main-project rule (and any captured id) is wiped before the
        # read. GetPolicyRule reads by policy_id ALONE (no project filter; owner bypasses RLS),
        # so seed its target in an ISOLATED project no version-activation touches → the row + its
        # CreatePolicyRule response id survive the whole run and stay Get-queryable.
        get_pol_project = f"{project}-getpolrule"
        try:
            created_rule = authz.CreatePolicyRule(
                authz_pb.CreatePolicyRuleRequest(subject=role_code, domain=tenant, object="ledger", action="data.update", effect=1, description="perf seed rule (version-isolated)", created_by=uid, tenant_id=tenant, project_id=get_pol_project),
                metadata=md, timeout=8.0,
            )
            pid = getattr(getattr(created_rule, "policy", None), "policy_id", "")
            if pid:
                fix.set("policy_id", pid)
        except (grpc.RpcError, AttributeError, ValueError):
            pass
        # A SEPARATE disposable rule (same isolated project) for the destructive DeletePolicyRule,
        # so deleting it never touches the GetPolicyRule target.
        try:
            del_rule = authz.CreatePolicyRule(
                authz_pb.CreatePolicyRuleRequest(subject=role_code, domain=tenant, object="ledger-disposable", action="data.delete", effect=1, description="perf seed disposable rule", created_by=uid, tenant_id=tenant, project_id=get_pol_project),
                metadata=md, timeout=8.0,
            )
            del_pid = getattr(getattr(del_rule, "policy", None), "policy_id", "")
            if del_pid:
                fix.set("delete_policy_id", del_pid)
        except (grpc.RpcError, AttributeError, ValueError):
            pass
        for fn in (
            lambda: authz.PutRoleBinding(authz_pb.PutRoleBindingRequest(binding=authz_pb.RoleBinding(subject=f"user:{uid}", role=role_code, tenant=tenant, project=project, source="sdk-perf")), metadata=md, timeout=8.0),
            lambda: authz.PutRelationship(authz_pb.PutRelationshipRequest(tuple=authz_pb.RelationshipTuple(subject=f"user:{uid}", relation="member", object=f"group:sdk-perf-{suffix}", tenant=tenant, project=project, source="sdk-perf")), metadata=md, timeout=8.0),
        ):
            try:
                fn()
            except (grpc.RpcError, AttributeError, ValueError):
                pass
    fix.set("relation", "member")
    fix.set("object", f"group:sdk-perf-{suffix}")
    fix.set("resource", "invoice")
    fix.set("action", "data.select")
    try:
        draft = authz.CreatePolicyDraft(
            authz_gov_pb.CreatePolicyDraftRequest(
                actor=authz_gov_pb.GovernanceActor(subject=_fixture(fix, "subject"), tenant_id=tenant, project_id=project, break_glass=True, break_glass_reason="sdk perf seed", break_glass_expires_at_unix=int(time.time()) + 900),
                tenant_id=tenant,
                project_id=project,
                policy_set_name="default",
                title=f"sdk perf draft {suffix}",
                change_reason="seed",
                document=authz_gov_pb.PolicyDocument(),
            ),
            metadata=md, timeout=8.0,
        )
        fix.set("policy_draft_id", draft.draft.draft_id)
    except grpc.RpcError:
        pass
    def governance_actor() -> authz_gov_pb.GovernanceActor:
        # Body actor.scopes are ignored by the live D1/D2 gate (it reads claim scopes,
        # and no role projects to authz:*), so the seed's own governance writes must use
        # the body-authoritative break-glass bypass — otherwise the drafts/versions/canary
        # are never created and the governance RPCs that read them fail "<id> is required".
        return authz_gov_pb.GovernanceActor(
            subject=_fixture(fix, "subject", f"user:{uid}"),
            tenant_id=tenant,
            project_id=project,
            break_glass=True,
            break_glass_reason="sdk perf seed",
            break_glass_expires_at_unix=int(time.time()) + 900,
        )

    def make_policy_draft(title: str) -> str:
        try:
            response = authz.CreatePolicyDraft(
                authz_gov_pb.CreatePolicyDraftRequest(
                    actor=governance_actor(),
                    tenant_id=tenant,
                    project_id=project,
                    policy_set_name="default",
                    title=f"{title}{suffix}",
                    change_reason="seed",
                    document=authz_gov_pb.PolicyDocument(),
                ),
                metadata=md, timeout=8.0,
            )
            if title == "sdk-perf-update-":
                fix.set("update_draft_updated_at_unix", str(response.draft.updated_at.seconds))
            return response.draft.draft_id
        except (grpc.RpcError, AttributeError):
            return ""

    update_draft_id = make_policy_draft("sdk-perf-update-")
    fix.set("update_draft_id", update_draft_id)
    approve_draft_id = make_policy_draft("sdk-perf-approve-")
    if approve_draft_id:
        try:
            authz.SubmitPolicyDraft(
                authz_gov_pb.SubmitPolicyDraftRequest(actor=governance_actor(), draft_id=approve_draft_id),
                metadata=md, timeout=8.0,
            )
            fix.set("approve_draft_id", approve_draft_id)
        except grpc.RpcError:
            pass
    reject_draft_id = make_policy_draft("sdk-perf-reject-")
    if reject_draft_id:
        try:
            authz.SubmitPolicyDraft(
                authz_gov_pb.SubmitPolicyDraftRequest(actor=governance_actor(), draft_id=reject_draft_id),
                metadata=md, timeout=8.0,
            )
            fix.set("reject_draft_id", reject_draft_id)
        except grpc.RpcError:
            pass

    def make_policy_version(set_name: str, title: str):
        try:
            response = authz.CreatePolicyDraft(
                authz_gov_pb.CreatePolicyDraftRequest(
                    actor=governance_actor(),
                    tenant_id=tenant,
                    project_id=project,
                    policy_set_name=set_name,
                    title=f"{title}{suffix}",
                    change_reason="seed",
                    document=authz_gov_pb.PolicyDocument(),
                ),
                metadata=md, timeout=8.0,
            )
            draft_id = response.draft.draft_id
            authz.SubmitPolicyDraft(
                authz_gov_pb.SubmitPolicyDraftRequest(actor=governance_actor(), draft_id=draft_id),
                metadata=md, timeout=8.0,
            )
            approved = authz.ApprovePolicyDraft(
                authz_gov_pb.ApprovePolicyDraftRequest(actor=governance_actor(), draft_id=draft_id, reviewer=uid, reason="seed approve"),
                metadata=md, timeout=8.0,
            )
            return approved.version
        except (grpc.RpcError, AttributeError):
            return None

    version = make_policy_version(f"sdk-perf-activate-set-{suffix}", "activate-")
    if version is not None:
        fix.set("policy_version_id", version.policy_version_id)
    canary_version = make_policy_version(f"sdk-perf-canary-set-{suffix}", "canary-")
    if canary_version is not None:
        fix.set("canary_version_id", canary_version.policy_version_id)
        try:
            canary = authz.ActivateCanary(
                authz_gov_pb.ActivateCanaryRequest(
                    actor=governance_actor(),
                    policy_version_id=canary_version.policy_version_id,
                    scope_kind=3,
                    scope_values=["10"],
                    # 1s window → promote-eligible ~1s after activation (promote_eligible:
                    # now-started >= success_window_secs). NOTE: 0 makes ActivateCanary
                    # substitute the large DEFAULT_CANARY_WINDOW_SECS, which never elapses
                    # during the run, so the measured PromoteCanary fails "window not elapsed".
                    success_window_secs=1,
                    metric_threshold=0.99,
                    min_samples=0,
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("canary_id", canary.canary.canary_id)
        except (grpc.RpcError, AttributeError):
            pass
    rollback_v1 = make_policy_version(f"sdk-perf-rollback-set-{suffix}", "rb1-")
    if rollback_v1 is not None:
        try:
            authz.ActivatePolicyVersion(
                authz_gov_pb.ActivatePolicyVersionRequest(actor=governance_actor(), policy_version_id=rollback_v1.policy_version_id),
                metadata=md, timeout=8.0,
            )
        except grpc.RpcError:
            pass
        rollback_v2 = make_policy_version(f"sdk-perf-rollback-set-{suffix}", "rb2-")
        if rollback_v2 is not None:
            try:
                authz.ActivatePolicyVersion(
                    authz_gov_pb.ActivatePolicyVersionRequest(actor=governance_actor(), policy_version_id=rollback_v2.policy_version_id),
                    metadata=md, timeout=8.0,
                )
                fix.set("rollback_policy_set_id", rollback_v2.policy_set_id)
                fix.set("rollback_target_version_id", rollback_v1.policy_version_id)
            except grpc.RpcError:
                pass

    # ── IdentityProviderService: a real OIDC provider -> provider_id ─────────────
    idp = clients[IdentityProviderServiceClient].stub
    try:
        provider = idp.CreateProvider(
            idp_pb.CreateProviderRequest(
                tenant_id=tenant,
                kind=idp_enum_pb.IDP_KIND_OIDC,
                display_name=f"SDK Perf OIDC {suffix}",
                issuer=f"https://idp.example.com/{suffix}",
                jwks_url="https://idp.example.com/jwks",
                client_ids=["perf-client"],
                audiences=["udb"],
                claim_mapping_json="{}",
                group_mapping_json='{"sdk-perf-group":"admin"}',
                jit_policy_json='{"require_verified_email":false}',
                account_linking_policy="explicit",
                enabled=True,
                created_by=uid,
                context=common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project)),
            ),
            metadata=md, timeout=8.0,
        )
        provider_id = provider.provider.provider_id
        fix.set("provider_id", provider_id)
        fix.set("scim_group_id", "sdk-perf-group")
        scim_ctx = common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project))
        try:
            disposable_provider = idp.CreateProvider(
                idp_pb.CreateProviderRequest(
                    tenant_id=tenant,
                    kind=idp_enum_pb.IDP_KIND_OIDC,
                    display_name=f"SDK Perf OIDC Disposable {suffix}",
                    issuer=f"https://idp-disposable.example.com/{suffix}",
                    jwks_url="https://idp-disposable.example.com/jwks",
                    client_ids=["perf-client-disposable"],
                    audiences=["udb"],
                    claim_mapping_json="{}",
                    group_mapping_json="{}",
                    jit_policy_json='{"require_verified_email":false}',
                    account_linking_policy="explicit",
                    enabled=True,
                    created_by=uid,
                    context=scim_ctx,
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("disable_provider_id", disposable_provider.provider.provider_id)
        except (grpc.RpcError, AttributeError):
            pass
        try:
            group = idp.ScimCreateGroup(
                idp_pb.ScimCreateGroupRequest(
                    tenant_id=tenant,
                    provider_id=provider_id,
                    scim_group_json=json.dumps({"displayName": "sdk-perf-group", "members": []}),
                    context=scim_ctx,
                ),
                metadata=md, timeout=8.0,
            )
            # ScimGetGroup resolves against the provider group_mapping_json key.
            # The SCIM resource id returned here is not accepted as that mapping id.
            _ = group
        except (grpc.RpcError, AttributeError):
            pass
        scim_user = f"scim-perf-user-{suffix}"
        try:
            scim_created = idp.ScimCreateUser(
                idp_pb.ScimCreateUserRequest(
                    tenant_id=tenant,
                    provider_id=provider_id,
                    scim_user_json=json.dumps({
                        "userName": scim_user,
                        "emails": [{"value": f"{scim_user}@example.com", "primary": True}],
                        "active": True,
                    }),
                    context=scim_ctx,
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("scim_user_id", scim_user)
            fix.set("external_identity_id", scim_created.user.id)
        except (grpc.RpcError, AttributeError):
            pass
        scim_delete_user = f"scim-perf-del-{suffix}"
        try:
            idp.ScimCreateUser(
                idp_pb.ScimCreateUserRequest(
                    tenant_id=tenant,
                    provider_id=provider_id,
                    scim_user_json=json.dumps({
                        "userName": scim_delete_user,
                        "emails": [{"value": f"{scim_delete_user}@example.com", "primary": True}],
                        "active": True,
                    }),
                    context=scim_ctx,
                ),
                metadata=md, timeout=8.0,
            )
            fix.set("delete_scim_user_id", scim_delete_user)
        except grpc.RpcError:
            pass
        try:
            saml = idp.CreateProvider(
                idp_pb.CreateProviderRequest(
                    tenant_id=tenant,
                    kind=idp_enum_pb.IDP_KIND_SAML,
                    display_name=f"SDK Perf SAML {suffix}",
                    issuer=f"https://saml.example.com/{suffix}",
                    jwks_url="https://saml.example.com/jwks",
                    client_ids=["perf-saml"],
                    audiences=["udb"],
                    claim_mapping_json="{}",
                    group_mapping_json="{}",
                    jit_policy_json='{"require_verified_email":false}',
                    account_linking_policy="explicit",
                    enabled=True,
                    created_by=uid,
                    context=scim_ctx,
                ),
                metadata=md, timeout=8.0,
            )
            saml_provider_id = saml.provider.provider_id
            fix.set("saml_provider_id", saml_provider_id)
            idp.ImportSamlMetadata(
                idp_pb.ImportSamlMetadataRequest(
                    provider_id=saml_provider_id,
                    tenant_id=tenant,
                    metadata_xml=SAML_IDP_METADATA_XML,
                    updated_by=uid,
                    context=scim_ctx,
                ),
                metadata=md, timeout=8.0,
            )
        except (grpc.RpcError, AttributeError):
            pass
    except grpc.RpcError:
        pass

    # ── ApiKeyService: a real key -> key_id + plain_key ───────────────────────────
    # Canonical-identity model: the key owner must be an EXISTING ACTIVE
    # SERVICE_ACCOUNT with an active typed grant, addressed by its UUID — a
    # bare service NAME is not a user_id and never was one.
    apikey = clients[ApiKeyServiceClient].stub
    svc_name = f"sdk-perf-svc-{suffix}"
    svc_owner = ""
    try:
        svc_user = authn.CreateUser(
            authn_pb2.CreateUserRequest(
                username=svc_name, email=f"{svc_name}@example.com", password=pw,
                tenant_id=tenant, project_id=project, full_name="SDK Perf Service Account",
                account_kind=2,  # ACCOUNT_KIND_SERVICE_ACCOUNT
            ),
            metadata=md, timeout=8.0,
        )
        svc_owner = svc_user.user.user_id
        # CreateUser persists PENDING_VERIFICATION; the typed grant and
        # CreateApiKey both require an ACTIVE service account.
        authn.ChangeUserStatus(
            authn_pb2.ChangeUserStatusRequest(
                user_id=svc_owner, new_status=2, reason="perf seed activate",
                context=common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project)),
            ),
            metadata=md, timeout=8.0,
        )
        authn.CreateServiceAccountGrant(
            authn_svc_pb2.CreateServiceAccountGrantRequest(
                tenant_id=tenant, user_id=svc_owner, service_identity=svc_name,
                project_id=project, approved_scopes=["data:read", "resource:read"], reason="sdk perf seed",
            ),
            metadata=md, timeout=8.0,
        )
        # The measured RevokeCertificateBinding revokes THIS seeded binding.
        binding = authn.CreateCertificateBinding(
            authn_svc_pb2.CreateCertificateBindingRequest(
                tenant_id=tenant, user_id=svc_owner, selector_kind="SPIFFE_URI",
                selector_value=f"spiffe://bench/seed-binding-{suffix}", reason="perf seed binding",
            ),
            metadata=md, timeout=8.0,
        )
        if binding.binding.binding_id:
            fix.set("grant_binding_id", binding.binding.binding_id)
    except grpc.RpcError:
        pass
    if not svc_owner:
        svc_owner = svc_name  # fall back; CreateApiKey fails typed, not INTERNAL
    # A SECOND ACTIVE service account WITHOUT a grant: the measured
    # CreateServiceAccountGrant makes its revision-1 grant here, and the
    # destructive-phase RotateServiceAccountIdentity rotates that same grant.
    svc_b_name = f"sdk-perf-svc-b-{suffix}"
    try:
        svc_b = authn.CreateUser(
            authn_pb2.CreateUserRequest(
                username=svc_b_name, email=f"{svc_b_name}@example.com", password=pw,
                tenant_id=tenant, project_id=project, full_name="SDK Perf Service Account B",
                account_kind=2,  # ACCOUNT_KIND_SERVICE_ACCOUNT
            ),
            metadata=md, timeout=8.0,
        )
        authn.ChangeUserStatus(
            authn_pb2.ChangeUserStatusRequest(
                user_id=svc_b.user.user_id, new_status=2, reason="perf seed activate",
                context=common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project)),
            ),
            metadata=md, timeout=8.0,
        )
        fix.set("grant_create_user_id", svc_b.user.user_id)
    except grpc.RpcError:
        pass
    key_ctx = common_pb.RequestContext(user_id=svc_owner, tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project))
    try:
        key = apikey.CreateApiKey(
            apikey_pb.CreateApiKeyRequest(name=f"sdk-perf-key-{suffix}", owner_id=svc_owner, scopes=["data:read"], context=key_ctx),
            metadata=md, timeout=8.0,
        )
        fix.set("key_id", key.key.key_id)
        fix.set("plain_key", key.plain_key)
        fix.set("owner_id", svc_owner)
    except grpc.RpcError:
        pass
    try:
        revoke_key = apikey.CreateApiKey(
            apikey_pb.CreateApiKeyRequest(name=f"sdk-perf-revoke-{suffix}", owner_id=svc_owner, scopes=["data:read"], context=key_ctx),
            metadata=md, timeout=8.0,
        )
        fix.set("revoke_key_id", revoke_key.key.key_id)
    except grpc.RpcError:
        pass
    try:
        update_key = apikey.CreateApiKey(
            apikey_pb.CreateApiKeyRequest(name=f"sdk-perf-update-{suffix}", owner_id=svc_owner, scopes=["data:read"], context=key_ctx),
            metadata=md, timeout=8.0,
        )
        fix.set("update_key_id", update_key.key.key_id)
    except grpc.RpcError:
        pass

    # ── AnalyticsService: a recorded metric -> a stage_name with data ─────────────
    analytics = clients[AnalyticsServiceClient].stub
    stage = f"sdk_perf_stage_{suffix}"
    try:
        analytics.RecordPipelineMetric(analytics_pb.RecordPipelineMetricRequest(stage_name=stage, tenant_id=tenant, latency_ms=100.0, is_success=True), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass
    fix.set("stage_name", stage)

    # ── NotificationService: template + a sent notification -> log_id, event_type ──
    notif = clients[NotificationServiceClient].stub
    event = f"sdk.perf.{suffix}"
    try:
        notif.UpsertTemplate(
            notif_pb.UpsertTemplateRequest(event_type=event, channel=NOTIFICATION_CHANNEL_EMAIL, locale="en", subject_template="SDK perf", body_template="sdk-perf-body", is_active=True),
            metadata=md, timeout=8.0,
        )
    except grpc.RpcError:
        pass
    fix.set("event_type", event)
    # Governance break-glass expiry: the D1/D2 governance gate reads scopes from the
    # VERIFIED claim, not request-body actor.scopes, and no role projects to authz:*
    # (tokens.rs ROLE_SCOPE_PROJECTIONS) — so the governance RPCs are reached via the
    # body-authoritative break-glass bypass (≤900s, reason-bearing, audited). Set at
    # seed time; the governance RPCs measure shortly after.
    fix.set("gov_exp", str(int(time.time()) + 900))
    if uid:
        try:
            sent = notif.SendNotification(
                notif_pb.SendNotificationRequest(
                    event_type=event,
                    recipient_id=uid,
                    recipient_address=f"sdk+{suffix}@example.com",
                    tenant_id=tenant,
                    resource_type="__perf_force_failed__",
                    channels=[NOTIFICATION_CHANNEL_EMAIL],
                ),
                metadata=md, timeout=8.0,
            )
            if sent.logs:
                log_id = sent.logs[0].log_id
                fix.set("log_id", log_id)
                fix.set("notification_id", log_id)
                # UDB_NOTIFICATION_TEST_MODE + ResourceType sentinel makes this
                # served send produce a real FAILED row for RetryNotification.
        except grpc.RpcError:
            pass
        try:
            notif.SetPreference(
                notif_pb.SetPreferenceRequest(user_id=uid, tenant_id=tenant, channel=NOTIFICATION_CHANNEL_EMAIL, is_opted_out=False),
                metadata=md, timeout=8.0,
            )
        except grpc.RpcError:
            pass
    fix.set("log_id", os.getenv("UDB_PERF_NOTIF_LOG", _fixture(fix, "log_id", "")))
    fix.set("notification_id", os.getenv("UDB_PERF_NOTIF_LOG", _fixture(fix, "notification_id", "")))

    # ── StorageService: a registered file -> file_id ──────────────────────────────
    storage = clients[StorageServiceClient].stub
    file_id = ""
    try:
        reg = storage.RegisterUpload(
            storage_pb.RegisterUploadRequest(tenant_id=tenant, project_id="", filename=f"perf-{suffix}.txt", content_type="text/plain", file_type=STORAGE_FILE_TYPE, reference_id=str(uuid.uuid4()), reference_type="sdk.perf", size_bytes=128, expires_in_minutes=30),
            metadata=md, timeout=8.0,
        )
        file_id = reg.file_id
        fix.set("file_id", file_id)
        file_body = f"sdk-perf-file-{suffix}".encode()
        fix.set("file_size_bytes", str(len(file_body)))
        # Upload through the StorageService-minted presigned URL so the bytes land in the
        # SAME object-plane target (backend/bucket/instance) that FinalizeUpload HEADs. Fall
        # back to the catalog-gated DataBroker PutObject only when no presigned URL exists.
        uploaded = False
        if reg.upload_url:
            try:
                put_presigned_storage_object(reg.upload_url, file_body, "text/plain")
                uploaded = True
            except Exception as exc:  # noqa: BLE001 — log + fall back, never fail the seed
                print(f"perf seed: presigned storage PUT failed: {exc}")
        if not uploaded:
            storage_bucket = os.getenv("UDB_STORAGE_BUCKET", "udb-storage")
            try:
                broker.EnsureResource(admin_pb2.ResourceAdminRequest(context=rc, backend="minio", resource_name=storage_bucket, spec_json="{}"), metadata=md, timeout=8.0)
                broker.PutObject(
                    iter([blob_pb2.Chunk(context=rc, bucket=storage_bucket, object_key=reg.object_key, data=file_body, content_type="text/plain", final_chunk=True)]),
                    metadata=md, timeout=8.0,
                )
            except grpc.RpcError:
                pass
        try:
            storage.FinalizeUpload(
                storage_pb.FinalizeUploadRequest(tenant_id=tenant, file_id=file_id, content_type="text/plain", file_type=STORAGE_FILE_TYPE, reference_id=file_id, reference_type="sdk.perf", size_bytes=len(file_body)),
                metadata=md, timeout=8.0,
            )
        except grpc.RpcError:
            pass
        try:
            delete_reg = storage.RegisterUpload(
                storage_pb.RegisterUploadRequest(tenant_id=tenant, project_id="", filename=f"perf-del-{suffix}.txt", content_type="text/plain", file_type=STORAGE_FILE_TYPE, reference_id=str(uuid.uuid4()), reference_type="sdk.perf", size_bytes=64, expires_in_minutes=30),
                metadata=md, timeout=8.0,
            )
            fix.set("delete_file_id", delete_reg.file_id)
        except grpc.RpcError:
            pass
        # A SEPARATE registered+uploaded but NOT finalized file for the measured
        # FinalizeUpload — finalizing the primary file_id again fails "already
        # finalized", so the measured Finalize needs its own un-finalized target.
        try:
            fin_reg = storage.RegisterUpload(
                storage_pb.RegisterUploadRequest(tenant_id=tenant, project_id="", filename=f"perf-fin-{suffix}.txt", content_type="text/plain", file_type=STORAGE_FILE_TYPE, reference_id=str(uuid.uuid4()), reference_type="sdk.perf", size_bytes=64, expires_in_minutes=30),
                metadata=md, timeout=8.0,
            )
            fin_file_id = fin_reg.file_id
            fix.set("finalize_file_id", fin_file_id)
            fin_body = f"sdk-perf-finalize-{suffix}".encode()
            fin_uploaded = False
            if fin_reg.upload_url:
                try:
                    put_presigned_storage_object(fin_reg.upload_url, fin_body, "text/plain")
                    fin_uploaded = True
                except Exception as exc:  # noqa: BLE001 — log + fall back, never fail the seed
                    print(f"perf seed: finalize presigned storage PUT failed: {exc}")
            if not fin_uploaded:
                storage_bucket = os.getenv("UDB_STORAGE_BUCKET", "udb-storage")
                try:
                    broker.PutObject(
                        iter([blob_pb2.Chunk(context=rc, bucket=storage_bucket, object_key=fin_reg.object_key, data=fin_body, content_type="text/plain", final_chunk=True)]),
                        metadata=md, timeout=8.0,
                    )
                except grpc.RpcError:
                    pass
            # NB: intentionally NOT finalized — the measured FinalizeUpload finalizes it.
            cleanups.append(lambda: storage.DeleteFile(storage_pb.DeleteFileRequest(tenant_id=tenant, file_id=fin_file_id), metadata=md, timeout=8.0))
        except grpc.RpcError:
            pass
        # A registered-but-PENDING upload (never uploaded, never finalized) for the
        # measured ReissueUploadUrl — it resumes a PENDING upload and rejects any
        # non-PENDING (finalized/ACTIVE) file, so it needs its own PENDING target.
        try:
            reissue_reg = storage.RegisterUpload(
                storage_pb.RegisterUploadRequest(tenant_id=tenant, project_id="", filename=f"perf-reissue-{suffix}.txt", content_type="text/plain", file_type=STORAGE_FILE_TYPE, reference_id=str(uuid.uuid4()), reference_type="sdk.perf", size_bytes=64, expires_in_minutes=30),
                metadata=md, timeout=8.0,
            )
            reissue_file_id = reissue_reg.file_id
            fix.set("reissue_file_id", reissue_file_id)
            cleanups.append(lambda: storage.DeleteFile(storage_pb.DeleteFileRequest(tenant_id=tenant, file_id=reissue_file_id), metadata=md, timeout=8.0))
        except grpc.RpcError:
            pass
        cleanups.append(lambda: storage.DeleteFile(storage_pb.DeleteFileRequest(tenant_id=tenant, file_id=file_id), metadata=md, timeout=8.0))
    except grpc.RpcError:
        pass

    # ── AssetService: pipeline definition + asset + a started instance ────────────
    if file_id:
        asset = clients[AssetServiceClient].stub
        definition_id = ""
        try:
            d = asset.CreatePipelineDefinition(
                asset_pb.CreatePipelineDefinitionRequest(tenant_id=tenant, name=f"sdk-perf-pipeline-{suffix}", description="perf seed", media_type="application/json", steps='[{"name":"extract","type":"EXTRACT"}]', version=1),
                metadata=md, timeout=8.0,
            )
            definition_id = d.definition_id
            fix.set("definition_id", definition_id)
        except grpc.RpcError:
            pass
        try:
            a = asset.RegisterAsset(
                asset_pb.RegisterAssetRequest(tenant_id=tenant, project_id="", file_id=file_id, name=f"sdk-perf-asset-{suffix}", media_type="application/json", metadata='{"source":"sdk-perf"}'),
                metadata=md, timeout=8.0,
            )
            fix.set("asset_id", a.asset_id)
            if definition_id:
                try:
                    inst = asset.StartPipeline(
                        asset_pb.StartPipelineRequest(tenant_id=tenant, definition_id=definition_id, asset_id=a.asset_id, context="{}", correlation_id=f"sdk-perf-{suffix}"),
                        metadata=md, timeout=8.0,
                    )
                    fix.set("instance_id", inst.instance_id)
                    try:
                        pipeline = asset.GetPipeline(asset_pb.GetPipelineRequest(tenant_id=tenant, instance_id=inst.instance_id), metadata=md, timeout=8.0)
                        if pipeline.steps:
                            fix.set("step_id", pipeline.steps[0].step_id)
                    except grpc.RpcError:
                        pass
                except grpc.RpcError:
                    pass
        except grpc.RpcError:
            pass

    # ── WebRTC: room + peer + track ───────────────────────────────────────────────
    rooms = clients[RoomServiceClient].stub
    peers = clients[PeerServiceClient].stub
    tracks = clients[TrackServiceClient].stub
    try:
        room = rooms.CreateRoom(
            webrtc_pb.CreateRoomRequest(tenant_id=tenant, name=f"sdk-perf-room-{suffix}", max_participants=8, config="{}", created_by=str(uuid.uuid4())),
            metadata=md, timeout=8.0,
        )
        room_id = room.room_id
        fix.set("room_id", room_id)
        cleanups.append(lambda: rooms.CloseRoom(webrtc_pb.CloseRoomRequest(tenant_id=tenant, room_id=room_id), metadata=md, timeout=8.0))
        try:
            joined = peers.JoinRoom(webrtc_pb.JoinRoomRequest(tenant_id=tenant, room_id=room_id, display_name="sdk-perf-peer", metadata="{}", user_agent="sdk-perf"), metadata=md, timeout=8.0)
            pid = joined.peer.peer_id
            fix.set("peer_id", pid)
            try:
                pub = tracks.PublishTrack(webrtc_pb.PublishTrackRequest(tenant_id=tenant, room_id=room_id, peer_id=pid, kind="audio", label="mic", settings="{}", metadata="{}"), metadata=md, timeout=8.0)
                fix.set("track_id", pub.track_id)
            except grpc.RpcError:
                pass
            try:
                pub2 = tracks.PublishTrack(webrtc_pb.PublishTrackRequest(tenant_id=tenant, room_id=room_id, peer_id=pid, kind="video", label="cam", settings="{}", metadata="{}"), metadata=md, timeout=8.0)
                fix.set("unpublish_track_id", pub2.track_id)
            except grpc.RpcError:
                pass
            try:
                leave_peer = peers.JoinRoom(webrtc_pb.JoinRoomRequest(tenant_id=tenant, room_id=room_id, display_name="sdk-perf-leave-peer", metadata="{}", user_agent="sdk-perf"), metadata=md, timeout=8.0)
                fix.set("leave_peer_id", leave_peer.peer.peer_id)
            except grpc.RpcError:
                pass
            try:
                signal_peer = peers.JoinRoom(webrtc_pb.JoinRoomRequest(tenant_id=tenant, room_id=room_id, display_name="sdk-perf-signal-peer", metadata="{}", user_agent="sdk-perf"), metadata=md, timeout=8.0)
                fix.set("signal_peer_id", signal_peer.peer.peer_id)
            except grpc.RpcError:
                pass
        except grpc.RpcError:
            pass
        try:
            close_room = rooms.CreateRoom(
                webrtc_pb.CreateRoomRequest(tenant_id=tenant, name=f"sdk-perf-close-room-{suffix}", max_participants=8, config="{}", created_by=str(uuid.uuid4())),
                metadata=md, timeout=8.0,
            )
            fix.set("close_room_id", close_room.room_id)
        except grpc.RpcError:
            pass
        # A SEPARATE high-capacity room for the measured JoinSession. The main room_id
        # is filled to capacity by JoinRoom's mutation iters (seeded peers + joins = the
        # cap of 8), so JoinSession against it would hit "room ... at capacity".
        try:
            js_room = rooms.CreateRoom(
                webrtc_pb.CreateRoomRequest(tenant_id=tenant, name=f"sdk-perf-joinsession-room-{suffix}", max_participants=64, config="{}", created_by=str(uuid.uuid4())),
                metadata=md, timeout=8.0,
            )
            js_room_id = js_room.room_id
            fix.set("join_session_room_id", js_room_id)
            cleanups.append(lambda: rooms.CloseRoom(webrtc_pb.CloseRoomRequest(tenant_id=tenant, room_id=js_room_id), metadata=md, timeout=8.0))
        except grpc.RpcError:
            pass
    except grpc.RpcError:
        pass

    # ── New native services: seed the ids/tokens their manifest bodies read ──────
    # Every ref below is required by a docs/bench-bodies manifest body (a
    # <seed:...> tag); without it perf_real_body raises MissingExplicitPerfBody
    # and aborts the whole run. Mirrors the Go perf seed (live_perf_seed_test.go).

    # VaultService: transit key + secrets → key/ciphertext/signature/secret-path refs.
    vault = clients[VaultServiceClient].stub
    vault_key = f"sdk-perf-key-{suffix}"
    fix.set("vault_key_name", vault_key)
    fix.set("vault_create_key_name", f"sdk-perf-create-key-{suffix}")
    fix.set("vault_db_role", "readonly")
    fix.set("vault_secret_path", f"app/config-{suffix}")
    fix.set("vault_put_secret_path", f"app/put-{suffix}")
    fix.set("vault_delete_secret_path", f"app/delete-{suffix}")
    fix.set("vault_destroy_secret_path", f"app/destroy-{suffix}")
    try:
        vault.CreateTransitKey(vault_pb.CreateTransitKeyRequest(tenant_id=tenant, key_name=vault_key, algorithm="aes256-gcm-siv"), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass
    # A dedicated ed25519 SIGNING key so GetTransitPublicKey exports a real public
    # key — the aes256-gcm-siv key above has no exportable public half.
    signing_key = f"sdk-perf-signing-key-{suffix}"
    fix.set("vault_signing_key_name", signing_key)
    try:
        vault.CreateTransitKey(vault_pb.CreateTransitKeyRequest(tenant_id=tenant, key_name=signing_key, algorithm="ed25519"), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass
    try:
        enc = vault.Encrypt(vault_pb.EncryptRequest(tenant_id=tenant, key_name=vault_key, plaintext="perf"), metadata=md, timeout=8.0)
        fix.set("vault_ciphertext", enc.ciphertext)
    except grpc.RpcError:
        pass
    try:
        sig = vault.Sign(vault_pb.SignRequest(tenant_id=tenant, key_name=vault_key, input="perf"), metadata=md, timeout=8.0)
        fix.set("vault_signature", sig.signature)
    except grpc.RpcError:
        pass
    # secret_path/delete/destroy are pre-created (measured reads/deletes need them);
    # put_secret_path is left unset so the measured PutSecret writes it fresh.
    for _path_key in ("vault_secret_path", "vault_delete_secret_path", "vault_destroy_secret_path"):
        try:
            vault.PutSecret(vault_pb.PutSecretRequest(tenant_id=tenant, secret_path=fix.m[_path_key], secret_value="perf-secret", expected_version=0, metadata_json="{}"), metadata=md, timeout=8.0)
        except grpc.RpcError:
            pass

    # LockService: two independent locks → renew/release fencing-token refs.
    locks = clients[LockServiceClient].stub
    lock_owner = _fixture(fix, "user_id", f"sdk-perf-owner-{suffix}")
    for _lock_ref, _lock_name in (
        ("renew_fencing_token", "sdk-perf-renew-lock"),
        ("release_fencing_token", "sdk-perf-release-lock"),
    ):
        try:
            acquired = locks.AcquireLock(lock_pb.AcquireLockRequest(tenant_id=tenant, lock_name=_lock_name, owner_id=lock_owner, lease_ttl_seconds=60, metadata_json="{}"), metadata=md, timeout=8.0)
            if acquired.fencing_token:
                fix.set(_lock_ref, str(acquired.fencing_token))
        except grpc.RpcError:
            pass

    # WorkflowService: primary + disposable workflow → workflow_id/cancel_workflow_id.
    workflow = clients[WorkflowServiceClient].stub
    for _wf_ref, _wf_type, _wf_corr in (
        ("workflow_id", "sdk.perf.workflow", record_id),
        ("cancel_workflow_id", "sdk.perf.cancel", f"cancel-{record_id}"),
    ):
        try:
            wf = workflow.StartWorkflow(workflow_pb.StartWorkflowRequest(tenant_id=tenant, project_id="", workflow_type=_wf_type, total_steps=20, payload="{}", compensations="[]", correlation_id=_wf_corr), metadata=md, timeout=8.0)
            fix.set(_wf_ref, wf.workflow_id)
        except grpc.RpcError:
            pass

    # SchedulerService: a stable job → job_id (reads/pause/resume/delete).
    scheduler = clients[SchedulerServiceClient].stub
    try:
        job = scheduler.CreateJob(scheduler_pb.CreateJobRequest(tenant_id=tenant, project_id="", name=f"sdk-perf-seed-job-{suffix}", schedule_type="CRON", cron_expression="*/5 * * * *", payload="{}", target_topic="sdk.perf.scheduler", max_attempts=3, backoff_seconds=30), metadata=md, timeout=8.0)
        fix.set("job_id", job.job_id)
    except grpc.RpcError:
        pass

    # WebhookService: primary + disposable endpoint → endpoint_id/delete_endpoint_id.
    webhooks = clients[WebhookServiceClient].stub
    for _ep_ref, _ep_url, _ep_desc in (
        ("endpoint_id", "https://example.com/udb-webhook-seed", "sdk perf seed webhook"),
        ("delete_endpoint_id", "https://example.com/udb-webhook-delete", "sdk perf delete webhook"),
    ):
        try:
            ep = webhooks.CreateEndpoint(webhook_pb.CreateEndpointRequest(tenant_id=tenant, url=_ep_url, topic_pattern="*", description=_ep_desc, max_attempts=3, metadata_json="{}"), metadata=md, timeout=8.0)
            fix.set(_ep_ref, ep.endpoint_id)
        except grpc.RpcError:
            pass

    # BackupService: policy + a tenant backup → backup_id + restore_tenant_id.
    backup = clients[BackupServiceClient].stub
    try:
        backup.PutBackupPolicy(backup_pb.PutBackupPolicyRequest(tenant_id=tenant, policy_name="sdk-perf-default", schedule_cron="0 3 * * *", retention_days=7, max_retained_backups=3, enabled=True, metadata_json="{}"), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass
    try:
        b = backup.StartTenantBackup(backup_pb.StartTenantBackupRequest(tenant_id=tenant, metadata_json='{"source":"sdk-perf-seed"}'), metadata=md, timeout=8.0)
        fix.set("backup_id", b.backup_id)
        fix.set("restore_tenant_id", str(uuid.uuid4()))
    except grpc.RpcError:
        pass

    # EmbeddingService: model registry, durable jobs, and one searchable vector.
    embedding = clients[EmbeddingServiceClient].stub
    def register_embedding_model(model_id: str, collection: str, alias: str) -> None:
        embedding.RegisterModel(
            embedding_pb.RegisterModelRequest(
                tenant_id=tenant, model_id=model_id, provider="deterministic",
                model_name="text-embedding-3-small", version="1", dimensions=3,
                matryoshka_dims=[3], distance_metric="COSINE", normalize=True,
                output_dtype="FLOAT32", max_input_tokens=8192, tokenizer="cl100k_base",
                task_type="DOCUMENT", provider_endpoint_ref="vault://embedding/sdk-live",
                vector_backend="qdrant", vector_instance="default", collection_alias=alias,
                active_collection=collection, chunking_strategy="TOKEN_RECURSIVE",
                chunk_tokens=256, chunk_overlap_tokens=32,
                metadata_json='{"suite":"sdk-live"}',
            ), metadata=md, timeout=8.0,
        )
    try:
        register_embedding_model("text-embedding-3-small", "sdk_live_records", "sdk_live_records_alias")
    except grpc.RpcError:
        pass
    embedding_delete_model_id = f"sdk-live-delete-model-{suffix}"
    fix.set("embedding_delete_model_id", embedding_delete_model_id)
    try:
        register_embedding_model(
            embedding_delete_model_id,
            f"sdk_live_delete_records_{suffix}",
            f"sdk_live_delete_records_alias_{suffix}",
        )
    except grpc.RpcError:
        pass
    try:
        embedding.RegisterSource(embedding_pb.RegisterSourceRequest(tenant_id=tenant, source_name="sdk_live_records", source_message_type=LIVE_MESSAGE_TYPE, text_fields=["payload"], target_collection="sdk_live_records", model_id="text-embedding-3-small", metadata_json="{}"), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass
    try:
        document = embedding.IngestDocument(
            embedding_pb.IngestDocumentRequest(
                tenant_id=tenant, external_id=f"sdk-live-work-{suffix}",
                title="SDK benchmark work fixture",
                raw_text="Durable embedding work is seeded from real document text for the SDK benchmark.",
                content_type="text/plain", doc_version="1", model_id="text-embedding-3-small",
                metadata_json='{"suite":"sdk-live","fixture":"work"}',
            ), metadata=md, timeout=8.0,
        )
        fix.set("embedding_job_id", document.job_id)
        work = embedding.ListEmbeddingWorkItems(
            embedding_pb.ListEmbeddingWorkItemsRequest(
                tenant_id=tenant, job_id=document.job_id, page_size=50,
            ), metadata=md, timeout=8.0,
        )
        if work.work_items:
            fix.set("embedding_work_item_id", work.work_items[0].work_item_id)
    except grpc.RpcError:
        pass
    try:
        parser = embedding.IngestDocument(
            embedding_pb.IngestDocumentRequest(
                tenant_id=tenant, external_id=f"sdk-live-parser-{suffix}",
                title="SDK benchmark parser fixture",
                storage_object_ref=f"udb://sdk-live/embedding-{suffix}.txt",
                content_type="text/plain", doc_version="1", model_id="text-embedding-3-small",
                metadata_json='{"suite":"sdk-live","fixture":"parser"}',
            ), metadata=md, timeout=8.0,
        )
        fix.set("embedding_document_id", parser.document_id)
        fix.set("embedding_document_job_id", parser.job_id)
    except grpc.RpcError:
        pass
    try:
        embedding.ReportEmbedding(embedding_pb.ReportEmbeddingRequest(tenant_id=tenant, source_name="sdk_live_records", row_pk=record_id, vector=[0.1, 0.2, 0.3], model="text-embedding-3-small", dims=3), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass

    # SearchService: create the seeded index so Search/Reindex/DeleteIndex resolve.
    search = clients[SearchServiceClient].stub
    try:
        search.CreateIndex(search_pb.CreateIndexRequest(tenant_id=tenant, index_name="sdk_live_records", source_message_type=LIVE_MESSAGE_TYPE, backend="qdrant", resource_name="sdk_live_records", vector_dims=3, metadata_json="{}"), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass

    # ControlPlaneService: open a StreamResources session under node_id so a node
    # state row exists and the control-plane perf bodies target served resources.
    control = clients[ControlPlaneServiceClient].stub
    node_id = f"sdk-perf-node-{suffix}"
    fix.set("node_id", node_id)
    try:
        req = control_pb.DiscoveryRequest(
            node_id=node_id,
            resource_type=control_enum_pb.RESOURCE_TYPE_BACKEND_TARGET_DEFINITION,
            context=common_pb.RequestContext(
                tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project),
                purpose="python.live.perf.seed",
            ),
        )
        stream = control.StreamResources(iter([req]), metadata=md, timeout=3.0)
        first = next(iter(stream), None)
        if first is not None:
            if first.version_info:
                fix.set("rollback_resource_version", first.version_info)
            if first.resources:
                fix.set("resource_name", first.resources[0].name)
        try:
            stream.cancel()
        except Exception:
            pass
    except Exception:
        pass

    # MeteringService: upsert the seeded quota so GetQuota/CheckQuota/QueryUsage read it.
    metering = clients[MeteringServiceClient].stub
    try:
        metering.PutQuota(metering_pb.PutQuotaRequest(tenant_id=tenant, project_id=project, metric="sdk.perf.request", limit_value=1000000, window_seconds=86400, enabled=True, metadata_json="{}"), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass

    # ConfigService: upsert the seeded flag so GetFlag/EvaluateFlags/ListFlags resolve.
    config = clients[ConfigServiceClient].stub
    try:
        config.PutFlag(config_pb.PutFlagRequest(tenant_id=tenant, project_id=project, environment="prod", flag_key="sdk.perf.enabled", value=config_pb.FlagValue(bool_value=True), enabled=True, rollout_percentage=100, rollout_context_key="user_id", metadata_json="{}"), metadata=md, timeout=8.0)
    except grpc.RpcError:
        pass

    # ── DataBroker migration/catalog lifecycle fixtures ─────────────────────────
    try:
        plan = broker.PlanMigration(
            admin_pb2.MigrationPlanRequest(context=rc, project_id=project, dry_run=True),
            metadata=md, timeout=8.0,
        )
        fix.set("migration_id", plan.run_id)
    except grpc.RpcError:
        pass
    try:
        approve_plan = broker.PlanMigration(
            admin_pb2.MigrationPlanRequest(context=meta.with_purpose("python.live.perf.seed.approve").to_request_context(), project_id=project, dry_run=False),
            metadata=md, timeout=8.0,
        )
        fix.set("approve_run_id", approve_plan.run_id)
    except grpc.RpcError:
        pass
    try:
        apply_plan = broker.PlanMigration(
            admin_pb2.MigrationPlanRequest(context=meta.with_purpose("python.live.perf.seed.apply").to_request_context(), project_id=project, dry_run=False),
            metadata=md, timeout=8.0,
        )
        _, call = broker.ApproveMigrationPlan.with_call(
            admin_pb2.MigrationRunRequest(context=meta.with_purpose("python.live.perf.seed.apply").to_request_context(), run_id=apply_plan.run_id, project_id=project),
            metadata=md, timeout=8.0,
        )
        fix.set("apply_run_id", apply_plan.run_id)
        for key, value in call.initial_metadata() or ():
            if key.lower() == "x-udb-approval-token":
                fix.set("approval_token", value)
                break
    except grpc.RpcError:
        pass
    try:
        manifest = broker.GetCatalogManifest(admin_pb2.CatalogManifestRequest(context=rc, redact=False), metadata=md, timeout=8.0)
        if manifest.manifest_json:
            fix.set("catalog_manifest", manifest.manifest_json.decode("utf-8"))
            fix.set("catalog_manifest_b64", base64.b64encode(manifest.manifest_json).decode("ascii"))
    except (grpc.RpcError, UnicodeDecodeError):
        pass
    try:
        broker.PutPolicy(
            admin_pb2.PutPolicyRequest(
                context=rc,
                policy=admin_pb2.PolicyRecord(effect="allow", tenant_id=tenant, priority=1, enabled=True),
            ),
            metadata=md, timeout=8.0,
        )
        policies = broker.ListPolicies(admin_pb2.PolicyListRequest(context=rc, include_disabled=True, limit=50), metadata=md, timeout=8.0)
        if policies.policies:
            fix.set("ds_policy_id", str(policies.policies[0].policy_id))
    except grpc.RpcError:
        pass

    def cleanup() -> None:
        for fn in reversed(cleanups):
            try:
                fn()
            except grpc.RpcError:
                pass

    return fix, record_id, cleanup


def time_cdc_first_event(broker_stub, method, meta: Metadata, record_id: str, timeout: float = 12.0) -> str:
    """Event-driven success path for PublishCDC: subscribe, then fire a real Upsert
    against the seeded SdkLiveRecord row (which flows outbox->CDC->Kafka) and read the
    FIRST delivered event. Returns "OK" on a delivered event, else the gRPC code name.
    The first-event latency is timed by the caller around this call."""
    md = meta.to_grpc_metadata()
    rc = meta.with_purpose("python.live.perf.cdc").to_request_context()
    try:
        stream = broker_stub.PublishCDC(cdc_pb2.CDCSubscriptionRequest(context=rc, topic_pattern="*"), metadata=md, timeout=timeout)
    except grpc.RpcError as exc:
        return exc.code().name
    try:
        broker_stub.Upsert(
            relational_pb2.UpsertRequest(
                context=meta.with_purpose("python.live.perf.cdc").to_request_context(), message_type=LIVE_MESSAGE_TYPE,
                record_json=live_record_json(record_id, meta.tenant_id, meta.project_id, "py-perf-cdc", "py-perf-cdc", int(time.time_ns())),
                conflict_fields=["record_id"],
            ),
            metadata=md, timeout=timeout,
        )
    except grpc.RpcError:
        pass
    try:
        next(iter(stream))  # block on the first delivered event (real produce->deliver round-trip)
        return "OK"
    except StopIteration:
        return "OK"
    except grpc.RpcError as exc:
        return exc.code().name
    finally:
        try:
            stream.cancel()
        except Exception:
            pass


def time_first_recv(
    rpc_callable,
    request,
    meta: Metadata,
    client_streaming: bool,
    server_streaming: bool,
    timeout: float = 12.0,
) -> str:
    """Open a non-CDC streaming RPC with a seeded request and read up to the FIRST
    server response (a real round-trip, not just stream-open). For client/bidi we send
    one seeded message and close the send side; for server-streaming we send the seeded
    request. Returns "OK" on first response (or an empty stream), else the gRPC code."""
    md = meta.to_grpc_metadata()
    stream = None
    try:
        if client_streaming:
            response_or_stream = rpc_callable(iter([request]), metadata=md, timeout=timeout)
        else:
            response_or_stream = rpc_callable(request, metadata=md, timeout=timeout)
        if client_streaming and not server_streaming:
            # stream-unary returns the unary response object, not an iterator.
            return "OK"
        stream = response_or_stream
        try:
            next(iter(stream))
            return "OK"
        except StopIteration:
            return "OK"
    except grpc.RpcError as exc:
        return exc.code().name
    except Exception as exc:
        return f"CLIENT_ERROR:{type(exc).__name__}"
    finally:
        if stream is not None:
            try:
                stream.cancel()
            except Exception:
                pass


WEBRTC_EGRESS_OPTIONAL_METHODS = {
    "ListEgress",
    "StartRoomComposite",
    "StartTrackEgress",
    "StopEgress",
}


def is_capability_skip(client_cls, method, err_code: str, err_detail: str = "") -> bool:
    if err_code != "FAILED_PRECONDITION":
        return False
    if client_cls is not RoomServiceClient or method.name not in WEBRTC_EGRESS_OPTIONAL_METHODS:
        return False
    detail = (err_detail or "").lower()
    return not detail or "webrtc egress" in detail or "egress is not enabled" in detail


def write_python_perf_report(samples, fixtures, authed_meta: Metadata, error: Exception | None = None) -> None:
    svc = {}
    for s in samples:
        svc.setdefault(s["service"], []).append(s["mean"])
    lines = ["# UDB SDK Live Perf — Python (localhost)", "",
             f"RPCs measured: {len(samples)}   tenant={authed_meta.tenant_id}", "",
             "Every RPC is driven down its SUCCESS path: a SEED phase first creates real, "
             "disposable entities (a user, role + assignment + policies, an API key, a notification, "
             "a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and "
             "the harness resolves each request's reference/ID fields to those real identifiers. So "
             "the numbers reflect real handler work, not validation-rejection latency. The TARGET is "
             "zero failures; any residual non-OK RPC is listed under Failures for the maintainer to "
             "finish.", "",
             "Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) "
             "report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, "
             "PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that "
             "flows outbox->CDC->Kafka, and times the first delivered event.", ""]
    if error is not None:
        detail = str(error).replace("\n", " ").replace("|", "\\|")
        lines += ["## Harness error", "",
                  f"`{type(error).__name__}`: {detail}", "",
                  "This is a partial report written before the benchmark process failed.", ""]
    lines += ["## Seeded fixtures", "",
              "Captured semantic field -> seeded value keys used to resolve request fields: "
              + ", ".join(sorted(fixtures.m)), "",
              "## Per-service mean latency", "", "| Service | RPCs | mean ms |", "|---|--:|--:|"]
    for name in sorted(svc, key=lambda k: -sum(svc[k]) / len(svc[k])):
        lines.append(f"| {name} | {len(svc[name])} | {sum(svc[name]) / len(svc[name]):.2f} |")
    # Failures subsection: every RPC whose last iteration returned a non-OK gRPC status.
    # A failing RPC is a FAILURE with its code, never a silent latency sample.
    failed = [s for s in samples if s["err"] not in {"OK", "CAPABILITY_SKIPPED"}]
    skipped = [s for s in samples if s["err"] == "CAPABILITY_SKIPPED"]
    lines += ["", f"## Failures ({len(failed)})", ""]
    if not failed:
        lines.append("No RPC returned a non-OK gRPC status.")
    else:
        lines.append("These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.")
        lines += ["", "| RPC | api_alias | operation_id | kind | err | detail | p99 ms | mean ms | iters |", "|---|---|---|---|---|---|--:|--:|--:|"]
        for s in sorted(failed, key=lambda x: (x["service"], x["rpc"])):
            detail = str(s.get("err_detail", "")).replace("\n", " ").replace("|", "\\|")
            lines.append(f"| {s['service']}/{s['rpc']} | {s['api_alias']} | {s['operation_id']} | {s['kind']} | {s['err']} | {detail} | {s['p99']:.2f} | {s['mean']:.2f} | {s['iters']} |")
    lines += ["", f"## Capability Skips ({len(skipped)})", ""]
    if not skipped:
        lines.append("No optional service capability was skipped.")
    else:
        lines.append("These RPCs reached the served path but require an optional backend capability disabled in this local profile.")
        lines += ["", "| RPC | api_alias | operation_id | kind | detail |", "|---|---|---|---|---|"]
        for s in sorted(skipped, key=lambda x: (x["service"], x["rpc"])):
            detail = str(s.get("err_detail", "")).replace("\n", " ").replace("|", "\\|")
            lines.append(f"| {s['service']}/{s['rpc']} | {s['api_alias']} | {s['operation_id']} | {s['kind']} | {detail} |")
    lines += ["", "## Slowest 20 by p99", "", "| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms |", "|---|---|---|---|---|--:|--:|--:|"]
    for s in sorted(samples, key=lambda x: -x["p99"])[:20]:
        lines.append(f"| {s['service']}/{s['rpc']} | {s['api_alias']} | {s['operation_id']} | {s['kind']} | {s['err']} | {s['p50']:.2f} | {s['p99']:.2f} | {s['mean']:.2f} |")
    lines += ["", "## Full per-RPC table (sorted by service, then RPC)", "", "| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |", "|---|---|---|---|---|---|--:|--:|--:|--:|"]
    for s in sorted(samples, key=lambda x: (x["service"], x["rpc"])):
        lines.append(f"| {s['service']} | {s['rpc']} | {s['api_alias']} | {s['operation_id']} | {s['kind']} | {s['err']} | {s['p50']:.2f} | {s['p99']:.2f} | {s['mean']:.2f} | {s['iters']} |")
    report = "\n".join(lines) + "\n"
    report_path = Path(__file__).resolve().parents[1] / "perf_report_python.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"\nPython perf: {len(samples)} RPCs measured, {len(failed)} FAILED (non-OK gRPC status) -> {report_path}")


def assert_not_mount_failure(label: str, exc: grpc.RpcError) -> None:
    if exc.code() in FATAL_CODES:
        raise AssertionError(
            f"{label} did not reach an implemented live RPC: {exc.code().name}: {exc.details()}"
        ) from exc


def drain_stream(label: str, iterator) -> None:
    deadline = time.monotonic() + 0.75
    while time.monotonic() < deadline:
        try:
            next(iterator)
            return
        except StopIteration:
            return
        except grpc.RpcError as exc:
            assert_not_mount_failure(label, exc)
            return


def test_live_generated_rpc_surface():
    target = required_env("UDB_GRPC_TARGET")
    auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", target)
    meta = metadata()

    auth = UdbAuthClient(auth_target, meta, timeout=10.0)
    login = auth.login(
        required_env("UDB_LIVE_USERNAME"),
        required_env("UDB_LIVE_PASSWORD"),
        device_name="python-sdk-live-conformance",
    )
    assert login.access_token
    assert login.refresh_token
    principal_resp = auth.authenticate_bearer(login.access_token)
    # Tenant-identity fix (auth_fix.md): bootstrap binds the admin to the tenant's
    # CANONICAL UUID, so the Login JWT tenant claim is a UUID — not the human code.
    # Discover it from our own authenticated principal and use it for every request
    # body, so the body tenant matches the claim and the UUID-strict services
    # (storage/webrtc/asset) accept it. ONE admin now serves the generated RPC surface.
    canonical_tenant = principal_resp.principal.tenant_id
    assert canonical_tenant, "authenticated principal must carry a tenant_id (the canonical UUID)"
    refreshed = auth.refresh_token(login.refresh_token)
    assert refreshed.access_token

    authed_meta = replace(metadata(bearer_token=login.access_token), tenant_id=canonical_tenant, client_catalog_version="")
    caps_stub = data_broker_pb2_grpc.DataBrokerStub(grpc.insecure_channel(target))
    caps = caps_stub.GetCapabilities(
        admin_pb2.CapabilitiesRequest(context=authed_meta.to_request_context(), project_id=authed_meta.project_id),
        metadata=authed_meta.to_grpc_metadata(),
        timeout=5.0,
    )
    enabled = {backend.lower() for backend in caps.enabled_backends}
    required = {
        item.strip().lower()
        for item in os.getenv("UDB_LIVE_REQUIRED_BACKENDS", "postgres,mongodb,minio").split(",")
        if item.strip()
    }
    assert required <= enabled, f"enabled_backends={sorted(enabled)} missing {sorted(required - enabled)}"

    # Don't trust the capability claim — exercise every advertised backend.
    run_backend_claim_check(caps_stub, authed_meta, enabled)

    # Challenge every advertised backend KIND's per-operation claims in BOTH directions.
    run_backend_capability_challenge(caps_stub, authed_meta, caps)

    # Full session lifecycle on a throwaway login: prove logout invalidates the
    # session (access token + refresh token + session-refresh all rejected after).
    # Deferred so the full-surface coverage probe still runs before any logout gap
    # fails the test.
    lifecycle_failures = run_auth_lifecycle(
        auth_target, authed_meta, required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD")
    )

    # Edge cases: the auth plane must fail CLOSED on bad credentials/forged bearers.
    run_auth_negative(auth_target, authed_meta, required_env("UDB_LIVE_USERNAME"))

    run_live_backend_e2e(caps_stub, authed_meta)

    # Per-RPC EDGE cases (fail-closed / no cross-tenant leak / no server fault).
    run_live_edge_cases(caps_stub, authed_meta)

    # Breadth: a real category-appropriate round-trip against EVERY advertised backend
    # kind (relational SQL, object, document, cache, vector, graph) — not just the
    # canonical postgres/mongodb/minio trio. Adapts to whatever the broker enabled.
    run_all_backend_kinds_matrix(caps_stub, authed_meta, caps)

    # Real create→read→assert CRUD against every native control-plane service.
    # A SINGLE admin (bound to the canonical tenant UUID) now serves the UUID-strict
    # services (storage/webrtc/asset) and the free-text ones alike — no second
    # "uuid tenant" admin needed (auth_fix.md tenant-identity fix).
    auth_channel = grpc.insecure_channel(auth_target)
    try:
        run_native_service_e2e(auth_channel, authed_meta)
    finally:
        auth_channel.close()

    # The full-surface generated RPC probe is now ONE parametrized test case per RPC
    # (`test_rpc_surface[...]`, below) so the runner reports per-RPC pass/fail like
    # the Go suite's sub-tests — not a single opaque "1 passed". This monolithic test
    # keeps the deep, value-asserted E2E (backend matrix, native-service CRUD, session
    # lifecycle, negative cases) above.
    if lifecycle_failures:
        raise AssertionError("; ".join(lifecycle_failures))


# --------------------------------------------------------------------------------
# Per-RPC surface coverage — one parametrized pytest case PER generated RPC, so
# the runner shows granular per-RPC results (like Go's sub-tests). Each case sends a
# descriptor-derived, field-populated typed request and asserts the RPC REACHED a
# live handler (no Unimplemented/Unavailable/Unknown mount failure) — i.e. it is
# wired, decodes, and validates. Login + clients are built ONCE via the module
# fixture. (Result-asserted deep E2E remains in the deep test above + native E2E.)
# --------------------------------------------------------------------------------

def _all_rpcs():
    rpcs = []
    for client_cls in SERVICE_CLIENTS:
        for method in service_descriptor(client_cls).methods:
            rpcs.append((client_cls, method))
    return rpcs


ALL_RPCS = _all_rpcs()
assert len(ALL_RPCS) == len(RPC_OPERATION_KIND), (
    f"expected {len(RPC_OPERATION_KIND)} RPCs from the descriptor set, found {len(ALL_RPCS)}"
)
RPC_ORDER_INDEX = {(client_cls, method.name): idx for idx, (client_cls, method) in enumerate(ALL_RPCS)}


def assert_explicit_perf_body_coverage() -> None:
    rows = bench_body_rows()
    missing_rows = []
    empty_bodies = []
    for client_cls, method in ALL_RPCS:
        body = doc_body_text(client_cls, method)
        if body is None:
            missing_rows.append(f"{client_cls._SERVICE_FULL}/{method.name}")
            continue
        fields = doc_field_names(client_cls, method)
        if method.input_type.fields and not fields and method.name != "GetJwks":
            empty_bodies.append(f"{client_cls._SERVICE_FULL}/{method.name}")
    assert len(rows) == len(ALL_RPCS), (
        f"docs/bench-bodies must have current generated RPC rows, found {len(rows)} want {len(ALL_RPCS)}"
    )
    assert not missing_rows, "missing docs/bench-bodies rows: " + ", ".join(missing_rows)
    assert not empty_bodies, "docs rows did not name any real request fields: " + ", ".join(empty_bodies)


assert_explicit_perf_body_coverage()

AUTH_FIRST_PERF = ["Login", "RefreshToken", "RefreshSession", "Authenticate", "ValidateToken", "IntrospectToken", "GetJwks"]
AUTH_FIRST_PERF_INDEX = {name: idx for idx, name in enumerate(AUTH_FIRST_PERF)}
AUTH_LAST_PERF = {
    "Logout", "RevokeSession", "RevokeDevice", "DisableMfaFactor", "RevokeRecoveryCodes",
    "DeleteWebAuthnCredential", "ChangePassword", "ResetPassword", "AdminResetPassword",
    "ChangeUserStatus", "AdminResetMfa", "AdminRevokeSession", "AdminRevokeAllUserSessions",
    "AdminRevokeAllTenantSessions", "EmergencyRevoke",
}

TERMINAL_PERF_METHODS = {
    "CloseRoom", "DeleteFile", "DeletePolicy", "DeletePolicyRule", "DeleteRole",
    "DisableProvider", "DismissDlqEvent", "DropResource", "LeaveRoom",
    "MuteTrack", "PromoteCanary", "QuarantineDlqEvent", "RejectPolicyDraft",
    "ReplayDlqEvent", "RevokeApiKey", "RevokeRole", "RollbackCatalog",
    "RollbackPolicyVersion", "PurgeTenant", "UnlinkIdentity", "UnpublishTrack",
}

SINGLE_CALL_PERF_METHODS = {
    "VerifyOTP", "VerifyMfaChallenge", "SendOTP", "ResendOTP", "ResetPassword",
    "ChangePassword", "RefreshToken", "RefreshSession", "Authenticate",
}


def perf_rpc_order_key(item):
    client_cls, method = item
    original_idx = RPC_ORDER_INDEX[(client_cls, method.name)]
    if client_cls is TenantServiceClient and method.name == "PurgeTenant":
        return (4, 0, original_idx)
    if client_cls is AuthnServiceClient and method.name in AUTH_FIRST_PERF_INDEX:
        return (0, AUTH_FIRST_PERF_INDEX[method.name])
    if client_cls is AuthnServiceClient and method.name in AUTH_LAST_PERF:
        return (3, original_idx)
    terminal = 1 if method.name in TERMINAL_PERF_METHODS else 0
    kind = RPC_OPERATION_KIND.get(rpc_path(method), "read_only")
    kind_rank = {"read_only": 0, "mutation": 1, "destructive": 2}.get(kind, 1)
    return (1 + terminal, kind_rank, original_idx)


@pytest.fixture(scope="module")
def live_session():
    target = required_env("UDB_GRPC_TARGET")
    auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", target)
    auth = UdbAuthClient(auth_target, metadata(), timeout=10.0)
    login = auth.login(
        required_env("UDB_LIVE_USERNAME"),
        required_env("UDB_LIVE_PASSWORD"),
        device_name="python-sdk-surface",
    )
    canonical_tenant = auth.authenticate_bearer(login.access_token).principal.tenant_id
    authed_meta = replace(metadata(bearer_token=login.access_token), tenant_id=canonical_tenant, client_catalog_version="")
    fixtures = PerfFixtures()
    fixtures.set("token", login.access_token)
    fixtures.set("access_token", login.access_token)
    fixtures.set("refresh_token", login.refresh_token)
    fixtures.set("session_id", login.session_id)
    clients = {}
    for client_cls in SERVICE_CLIENTS:
        endpoint = target if client_cls is DataBrokerClient else auth_target
        clients[client_cls] = client_cls(endpoint, authed_meta, timeout=10.0)
    try:
        yield {"authed_meta": authed_meta, "clients": clients, "fixtures": fixtures}
    finally:
        for client in clients.values():
            client.close()


@pytest.mark.parametrize(
    "rpc", ALL_RPCS, ids=[f"{cc._SERVICE_FULL}/{m.name}" for cc, m in ALL_RPCS]
)
def test_rpc_surface(live_session, rpc):
    client_cls, method = rpc
    authed_meta = live_session["authed_meta"]
    client = live_session["clients"][client_cls]
    fixtures = live_session["fixtures"]
    label = f"{client_cls._SERVICE_FULL}/{method.name}"
    request = perf_real_body(client_cls, method, authed_meta, fixtures)
    rpc_callable = getattr(client.stub, method.name)
    try:
        if method.client_streaming:
            iterator = rpc_callable(iter([request]), metadata=authed_meta.to_grpc_metadata(), timeout=10.0)
            if method.server_streaming:
                drain_stream(label, iterator)
        elif method.server_streaming:
            drain_stream(label, rpc_callable(request, metadata=authed_meta.to_grpc_metadata(), timeout=10.0))
        else:
            rpc_callable(request, metadata=authed_meta.to_grpc_metadata(), timeout=10.0)
    except grpc.RpcError as exc:
        assert_not_mount_failure(label, exc)


# --------------------------------------------------------------------------------
# Per-RPC performance (gated on UDB_LIVE_PERF=1). Times every RPC over multiple
# iterations and writes perf_report_python.md — the Python counterpart of the Go
# perf harness. read_only RPCs are timed many times; mutations a few; destructive
# once with explicit bodies.
# --------------------------------------------------------------------------------

@pytest.mark.skipif(os.getenv("UDB_LIVE_PERF") != "1", reason="perf run requires UDB_LIVE_PERF=1")
def test_live_perf():
    samples = []
    fixtures = PerfFixtures()
    authed_meta = metadata()
    clients = {}
    seed_cleanup = lambda: None
    try:
        target = required_env("UDB_GRPC_TARGET")
        auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", target)
        auth = UdbAuthClient(auth_target, metadata(), timeout=10.0)
        login = auth.login(required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD"), device_name="python-sdk-perf")
        refreshed = auth.refresh_token(login.refresh_token, session_id=login.session_id)
        login_access_token = refreshed.access_token or login.access_token
        canonical_tenant = auth.authenticate_bearer(login_access_token).principal.tenant_id
        authed_meta = replace(metadata(bearer_token=login_access_token), tenant_id=canonical_tenant, client_catalog_version="")
        for client_cls in SERVICE_CLIENTS:
            endpoint = target if client_cls is DataBrokerClient else auth_target
            clients[client_cls] = client_cls(endpoint, authed_meta, timeout=20.0)

        # SEED PHASE (runs before any measurement): create real, disposable entities and
        # capture their identifiers so every RPC can be driven down its SUCCESS path with
        # valid inputs. ``authed_meta.tenant_id`` is the canonical tenant UUID, so the one
        # bearer serves the UUID-strict native services (storage/asset/webrtc) too.
        fixtures, seed_record_id, seed_cleanup = perf_seed(clients, authed_meta)
        fixtures.set("token", login_access_token)
        fixtures.set("access_token", login_access_token)
        fixtures.set("refresh_token", login.refresh_token)
        fixtures.set("session_id", login.session_id)
        fixtures.set("csrf_token", login.csrf_token)

        broker_stub = clients[DataBrokerClient].stub
        def fresh_login(device_name: str):
            return clients[AuthnServiceClient].stub.Login(
                authn_pb2.LoginRequest(
                    username=required_env("UDB_LIVE_USERNAME"),
                    password=required_env("UDB_LIVE_PASSWORD"),
                    tenant_hint=authed_meta.tenant_id,
                    project_hint=authed_meta.project_id,
                    device_name=device_name,
                ),
                metadata=authed_meta.to_grpc_metadata(),
                timeout=8.0,
            )

        try:
            token_login = fresh_login("python-sdk-perf-token")
            fixtures.set("token", token_login.access_token)
            fixtures.set("access_token", token_login.access_token)
            fixtures.set("csrf_token", token_login.csrf_token)
        except grpc.RpcError:
            pass
        try:
            refresh_login = fresh_login("python-sdk-perf-refresh")
            fixtures.set("refresh_token", refresh_login.refresh_token)
        except grpc.RpcError:
            pass
        try:
            session_login = fresh_login("python-sdk-perf-session")
            fixtures.set("session_id", session_login.session_id)
        except grpc.RpcError:
            pass
        try:
            auto_login = fresh_login("python-sdk-perf-auto-refresh")
            auto_refresh_token = auto_login.refresh_token
            auto_session_id = auto_login.session_id
        except grpc.RpcError:
            auto_refresh_token = refreshed.refresh_token or login.refresh_token
            auto_session_id = login.session_id
        next_token_refresh_at = time.monotonic() + 180.0

        def ensure_fresh_perf_token(force: bool = False) -> None:
            nonlocal authed_meta, auto_refresh_token, auto_session_id, next_token_refresh_at
            if not force and time.monotonic() < next_token_refresh_at:
                return
            refreshed_token = auth.refresh_token(auto_refresh_token, session_id=auto_session_id)
            if refreshed_token.access_token:
                authed_meta = replace(authed_meta, bearer_token=refreshed_token.access_token, client_catalog_version="")
                fixtures.set("token", refreshed_token.access_token)
                fixtures.set("access_token", refreshed_token.access_token)
                for bound_client in clients.values():
                    bound_client.bind_metadata(authed_meta)
            if refreshed_token.refresh_token:
                auto_refresh_token = refreshed_token.refresh_token
            next_token_refresh_at = time.monotonic() + max(60.0, min(float(refreshed_token.access_token_expires_in or 240) * 0.6, 240.0))

        def is_cdc_subscription(client_cls, method) -> bool:
            return client_cls is DataBrokerClient and method.name == "PublishCDC"

        def iters_for(kind):
            return 1 if kind == "destructive" else (3 if kind == "mutation" else 10)

        def time_one(client, client_cls, method):
            # Returns (elapsed_ms, err_code, err_detail) where err_code is the gRPC
            # status code NAME on a non-OK status, else "OK".
            # A failing RPC must never be reported as a silent latency sample.
            #
            # Every measured RPC must have an explicit doc-backed body. No reflective
            # fallback is allowed here; missing coverage is a harness error.
            ensure_fresh_perf_token()
            rpc_callable = getattr(client.stub, method.name)
            start = time.perf_counter()
            err_code = "OK"
            err_detail = ""
            if is_cdc_subscription(client_cls, method):
                # Event-driven success path: subscribe, fire a real seeded Upsert
                # (outbox->CDC->Kafka), and time the first delivered event.
                err_code = time_cdc_first_event(broker_stub, method, authed_meta, seed_record_id)
                return (time.perf_counter() - start) * 1000.0, err_code, err_detail
            try:
                request = perf_real_body(client_cls, method, authed_meta, fixtures)
            except MissingExplicitPerfBody as exc:
                # An RPC whose body needs a fixture the seed could not populate
                # (e.g. a service was degraded/unmounted during seeding). Record it
                # as a skip rather than aborting the WHOLE perf run — a single
                # unseeded RPC must not lose every other service's samples.
                return (time.perf_counter() - start) * 1000.0, "SKIP_NO_BODY", str(exc)
            if method.client_streaming or method.server_streaming:
                # Other streaming RPCs: open with seeded inputs and measure time to the
                # FIRST server response (a real round-trip), not just stream-open.
                err_code = time_first_recv(
                    rpc_callable, request, authed_meta, method.client_streaming, method.server_streaming
                )
                if is_capability_skip(client_cls, method, err_code):
                    err_code = "CAPABILITY_SKIPPED"
                return (time.perf_counter() - start) * 1000.0, err_code, err_detail
            try:
                rpc_callable(request, metadata=authed_meta.to_grpc_metadata(), timeout=20.0)
            except grpc.RpcError as exc:
                try:
                    err_code = exc.code().name
                except Exception:
                    err_code = "UNKNOWN"
                try:
                    err_detail = exc.details() or ""
                except Exception:
                    err_detail = str(exc)
            if is_capability_skip(client_cls, method, err_code, err_detail):
                err_code = "CAPABILITY_SKIPPED"
            if err_code == "OK" and client_cls is AuthnServiceClient and method.name == "PutMfaPolicy":
                try:
                    reset = authn_pb2.PutMfaPolicyRequest()
                    reset.CopyFrom(request)
                    reset.require_mfa = False
                    rpc_callable(reset, metadata=authed_meta.to_grpc_metadata(), timeout=20.0)
                except grpc.RpcError:
                    pass
            return (time.perf_counter() - start) * 1000.0, err_code, err_detail

        # The current generated RPC surface is measured down the SUCCESS path. Unary = full round-trip;
        # non-CDC streaming = time-to-first-response (seeded inputs); CDC subscription
        # (PublishCDC) = time-to-first-event (subscribe, fire a real Upsert, time delivery).
        for client_cls, method in sorted(ALL_RPCS, key=perf_rpc_order_key):
            client = clients[client_cls]
            streaming = method.client_streaming or method.server_streaming
            if is_cdc_subscription(client_cls, method):
                kind, n = "cdc_first_event", 1
            elif streaming:
                kind, n = "stream_first_recv", 3
            else:
                kind = RPC_OPERATION_KIND.get(rpc_path(method), "read_only")
                n = iters_for(kind)
                if method.name in SINGLE_CALL_PERF_METHODS:
                    n = 1
            print(f"[PERF-RPC] {client_cls._SERVICE_FULL}/{method.name} kind={kind} iters={n}", flush=True)
            runs = []
            if kind == "read_only" and not streaming and method.name not in SINGLE_CALL_PERF_METHODS:
                warm = time_one(client, client_cls, method)  # warm-up
                if warm[1] != "OK":
                    runs = [warm]
            while len(runs) < n:
                run = time_one(client, client_cls, method)
                runs.append(run)
                if run[1] != "OK":
                    break
            all_durs = [d for d, _, _ in runs]
            ok_durs = [d for d, code, _ in runs if code == "OK"]
            err_code = "OK" if ok_durs else next((code for _, code, _ in runs if code != "OK"), "UNKNOWN")
            err_detail = "" if ok_durs else next((detail for _, code, detail in runs if code != "OK"), "")
            durs = sorted(ok_durs or all_durs)

            def pct(p, durs=durs):
                return durs[min(len(durs) - 1, (p * (len(durs) - 1)) // 100)]

            samples.append({
                "service": client_cls._SERVICE_FULL.split(".")[-1],
                "rpc": method.name,
                "api_alias": RPC_API_ALIAS.get(rpc_path(method), ""),
                "operation_id": RPC_OPERATION_ID.get(rpc_path(method), ""),
                "kind": kind, "iters": n, "err": err_code,
                "err_detail": err_detail,
                "p50": pct(50), "p99": pct(99), "mean": sum(durs) / len(durs),
            })
            if err_code != "OK" and err_code != "CAPABILITY_SKIPPED":
                print(f"[PERF-FAIL] {client_cls._SERVICE_FULL}/{method.name} => {err_code}: {err_detail}")
            else:
                print(f"[PERF-OK] {client_cls._SERVICE_FULL}/{method.name}", flush=True)
    except Exception as exc:
        write_python_perf_report(samples, fixtures, authed_meta, exc)
        raise
    finally:
        try:
            seed_cleanup()
        except Exception:
            pass
        for c in clients.values():
            c.close()

    write_python_perf_report(samples, fixtures, authed_meta)
