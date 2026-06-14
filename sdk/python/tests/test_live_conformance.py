from __future__ import annotations

import importlib
import json
import os
import pkgutil
import time
import uuid
from dataclasses import replace

import grpc
import pytest
from google.protobuf import struct_pb2
from google.protobuf.message_factory import GetMessageClass

from udb.services.v1 import data_broker_pb2, data_broker_pb2_grpc
from udb.core.authn.services.v1 import core_pb2 as authn_pb2
from udb.core.authn.services.v1 import authn_service_pb2_grpc as authn_grpc
from udb.entity.v1 import admin_pb2, blob_pb2, operation_pb2, relational_pb2, stores_pb2, vector_pb2

# Native control-plane service messages + stubs (real CRUD, not just mount probes).
from udb.core.common.v1 import types_pb2 as common_pb, dto_pb2 as common_dto_pb
from udb.core.tenant.services.v1 import tenant_service_pb2 as tenant_pb, tenant_service_pb2_grpc as tenant_grpc
from udb.core.authz.services.v1 import core_pb2 as authz_pb, authz_service_pb2_grpc as authz_grpc
from udb.core.apikey.services.v1 import core_pb2 as apikey_pb, apikey_service_pb2_grpc as apikey_grpc
from udb.core.analytics.services.v1 import core_pb2 as analytics_pb, analytics_service_pb2_grpc as analytics_grpc
from udb.core.notification.services.v1 import core_pb2 as notif_pb, notification_service_pb2_grpc as notif_grpc
from udb.core.storage.services.v1 import storage_service_pb2 as storage_pb, storage_service_pb2_grpc as storage_grpc
from udb.core.asset.services.v1 import asset_service_pb2 as asset_pb, asset_service_pb2_grpc as asset_grpc
from udb.core.webrtc.services.v1 import webrtc_service_pb2 as webrtc_pb, webrtc_service_pb2_grpc as webrtc_grpc

from udb_client.auth import UdbAuthClient
from udb_client.generated_client import (
    RPC_OPERATION_KIND,
    AnalyticsServiceClient,
    ApiKeyServiceClient,
    AssetServiceClient,
    AuthnServiceClient,
    AuthzServiceClient,
    ControlPlaneServiceClient,
    DataBrokerClient,
    IdentityProviderServiceClient,
    NotificationServiceClient,
    PeerServiceClient,
    RoomServiceClient,
    SignalingServiceClient,
    StorageServiceClient,
    TenantServiceClient,
    TrackServiceClient,
    TurnServiceClient,
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
    ControlPlaneServiceClient,
    IdentityProviderServiceClient,
    NotificationServiceClient,
    StorageServiceClient,
    TenantServiceClient,
    PeerServiceClient,
    RoomServiceClient,
    SignalingServiceClient,
    TrackServiceClient,
    TurnServiceClient,
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
            subject_template="SDK {{n}}", body_template=body, is_active=True,
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


from google.protobuf.descriptor import FieldDescriptor as _FD

_INT_FD_TYPES = {
    _FD.TYPE_INT32, _FD.TYPE_INT64, _FD.TYPE_UINT32, _FD.TYPE_UINT64,
    _FD.TYPE_SINT32, _FD.TYPE_SINT64, _FD.TYPE_FIXED32, _FD.TYPE_FIXED64,
    _FD.TYPE_SFIXED32, _FD.TYPE_SFIXED64,
}


def rpc_path(method) -> str:
    """Full gRPC path "/pkg.Service/Method" for a proto MethodDescriptor."""
    return f"/{method.containing_service.full_name}/{method.name}"


def should_populate(method) -> bool:
    """Field-populate every RPC except DESTRUCTIVE ones, classified by the
    proto-derived RPC_OPERATION_KIND from the generated client — never a hardcoded
    name list. A populated destructive RPC (PutPolicy, RollbackCatalog,
    revoke-all/emergency/reset, DropResource, …) could corrupt shared state."""
    return RPC_OPERATION_KIND.get(rpc_path(method)) != "destructive"


def _probe_string(name: str, tenant: str, project: str) -> str:
    n = name.lower()
    if "tenant" in n:
        return tenant
    if "project" in n:
        return project
    if n == "message_type" or "messagetype" in n:
        return LIVE_MESSAGE_TYPE
    if "domain" in n:
        return tenant
    if "purpose" in n:
        return "python.live.probe"
    if "page_token" in n or "pagetoken" in n:
        return ""
    return "sdk-live-probe"


def populate_context(ctx_msg, full: str, tenant: str, project: str) -> None:
    try:
        if full == "udb.core.common.v1.RequestContext":
            ctx_msg.tenant.tenant_id = tenant
            ctx_msg.tenant.project_id = project
            ctx_msg.purpose = "python.live.probe"
        else:
            ctx_msg.tenant_id = tenant
            ctx_msg.project_id = project
            ctx_msg.purpose = "python.live.probe"
    except (AttributeError, ValueError, TypeError):
        pass


def populate_probe(msg, tenant: str, project: str, depth: int = 0) -> None:
    """Field-populate a read RPC's request so the probe exercises real decode +
    validation + handler logic across the full surface (not just an empty ping)."""
    for f in msg.DESCRIPTOR.fields:
        if f.label == _FD.LABEL_REPEATED or f.name == "context":
            continue
        try:
            if f.type == _FD.TYPE_STRING:
                setattr(msg, f.name, _probe_string(f.name, tenant, project))
            elif f.type in _INT_FD_TYPES:
                setattr(msg, f.name, 1)
            elif f.type in (_FD.TYPE_DOUBLE, _FD.TYPE_FLOAT):
                setattr(msg, f.name, 1.0)
            elif f.type == _FD.TYPE_MESSAGE:
                full = f.message_type.full_name
                if full in ("udb.entity.v1.RequestContext", "udb.core.common.v1.RequestContext"):
                    populate_context(getattr(msg, f.name), full, tenant, project)
                elif full.startswith("google.protobuf."):
                    pass
                elif depth < 1:
                    populate_probe(getattr(msg, f.name), tenant, project, depth + 1)
        except (AttributeError, ValueError, TypeError):
            pass  # best-effort: a populate mismatch must never break the probe


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


def default_request(method, meta: Metadata):
    request = GetMessageClass(method.input_type)()
    ctx_field = method.input_type.fields_by_name.get("context")
    if ctx_field is not None and ctx_field.message_type is not None:
        src = meta.to_request_context()
        # Two distinct `RequestContext` messages exist (entity.v1 for the data
        # plane vs core.common.v1 for the auth/control plane). Only copy when the
        # method's `context` field actually matches our builder's type; otherwise
        # leave it default — a mount probe just needs the RPC to be reachable, and
        # the broker authorizes from the bearer JWT, not the body context, so a
        # validation error (not a mount-failure code) is fine.
        if ctx_field.message_type.full_name == src.DESCRIPTOR.full_name:
            request.context.CopyFrom(src)
    # Deepen the full-surface probe: read RPCs get a field-populated request.
    if should_populate(method):
        populate_probe(request, meta.tenant_id, meta.project_id)
    return request


def perf_real_body(method, meta: Metadata):
    """A SEMANTICALLY VALID body for the top data-plane CRUD RPCs so the perf
    harness measures REAL handler work (a real Upsert/Select against the built-in
    ``udb.sdk.live.v1.SdkLiveRecord`` schema, always active) instead of
    validation-rejection on an empty/placeholder request. Upsert uses a FIXED
    record_id so repeated iterations are idempotent (no row accumulation). Returns
    ``None`` for RPCs without an override — the caller falls back to
    ``default_request``. Only DataBroker exposes Select/Upsert, so matching on the
    method name is unambiguous.
    """
    if method.name == "Upsert":
        return relational_pb2.UpsertRequest(
            context=meta.to_request_context(),
            message_type=LIVE_MESSAGE_TYPE,
            record_json=live_record_json(
                f"py-perf-{meta.tenant_id}-{meta.project_id}",
                meta.tenant_id,
                meta.project_id,
                "py-perf-lk",
                "py-perf",
                1,
            ),
            conflict_fields=["record_id"],
        )
    if method.name == "Select":
        return relational_pb2.SelectRequest(
            context=meta.to_request_context(),
            message_type=LIVE_MESSAGE_TYPE,
            filter=live_struct({"tenant_id": meta.tenant_id, "project_id": meta.project_id}),
            limit=10,
        )
    return None


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
    # (storage/webrtc/asset) accept it. ONE admin now serves all 262 RPCs.
    canonical_tenant = principal_resp.principal.tenant_id
    assert canonical_tenant, "authenticated principal must carry a tenant_id (the canonical UUID)"
    refreshed = auth.refresh_token(login.refresh_token)
    assert refreshed.access_token

    authed_meta = replace(metadata(bearer_token=login.access_token), tenant_id=canonical_tenant)
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

    # The full-surface 262-RPC probe is now ONE parametrized test case per RPC
    # (`test_rpc_surface[...]`, below) so the runner reports per-RPC pass/fail like
    # the Go suite's sub-tests — not a single opaque "1 passed". This monolithic test
    # keeps the deep, value-asserted E2E (backend matrix, native-service CRUD, session
    # lifecycle, negative cases) above.
    if lifecycle_failures:
        raise AssertionError("; ".join(lifecycle_failures))


# --------------------------------------------------------------------------------
# Per-RPC surface coverage — one parametrized pytest case PER RPC (262 total), so
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
assert len(ALL_RPCS) == 262, f"expected 262 RPCs from the descriptor set, found {len(ALL_RPCS)}"


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
    authed_meta = replace(metadata(bearer_token=login.access_token), tenant_id=canonical_tenant)
    clients = {}
    for client_cls in SERVICE_CLIENTS:
        endpoint = target if client_cls is DataBrokerClient else auth_target
        clients[client_cls] = client_cls(endpoint, authed_meta, timeout=10.0)
    try:
        yield {"authed_meta": authed_meta, "clients": clients}
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
    label = f"{client_cls._SERVICE_FULL}/{method.name}"
    request = default_request(method, authed_meta)
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
# once typed-empty (validation latency only).
# --------------------------------------------------------------------------------

@pytest.mark.skipif(os.getenv("UDB_LIVE_PERF") != "1", reason="perf run requires UDB_LIVE_PERF=1")
def test_live_perf():
    target = required_env("UDB_GRPC_TARGET")
    auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", target)
    auth = UdbAuthClient(auth_target, metadata(), timeout=10.0)
    login = auth.login(required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD"), device_name="python-sdk-perf")
    canonical_tenant = auth.authenticate_bearer(login.access_token).principal.tenant_id
    authed_meta = replace(metadata(bearer_token=login.access_token), tenant_id=canonical_tenant)
    clients = {}
    for client_cls in SERVICE_CLIENTS:
        endpoint = target if client_cls is DataBrokerClient else auth_target
        clients[client_cls] = client_cls(endpoint, authed_meta, timeout=20.0)

    def iters_for(kind):
        return 1 if kind == "destructive" else (5 if kind == "mutation" else 25)

    def time_one(client, method):
        # Returns (elapsed_ms, err_code) where err_code is the gRPC status code NAME
        # (e.g. "UNAVAILABLE", "FAILED_PRECONDITION") on a non-OK status, else "OK".
        # A failing RPC must never be reported as a silent latency sample.
        # Top data-plane CRUD RPCs get a real, valid body (real e2e handler work);
        # everything else falls back to the generic typed request.
        request = perf_real_body(method, authed_meta) or default_request(method, authed_meta)
        rpc_callable = getattr(client.stub, method.name)
        streaming = method.client_streaming or method.server_streaming
        start = time.perf_counter()
        err_code = "OK"
        try:
            if method.client_streaming:
                # Open the client/bidi stream and push one message, then cancel WITHOUT
                # reading responses: a subscription/upload stream has no first-message
                # latency in a passive run — draining it would just hit the deadline.
                call = rpc_callable(iter([request]), metadata=authed_meta.to_grpc_metadata(), timeout=5.0)
                call.cancel()
            elif method.server_streaming:
                # Open the server stream, do NOT drain (PublishCDC etc. emit only on events).
                call = rpc_callable(request, metadata=authed_meta.to_grpc_metadata(), timeout=5.0)
                call.cancel()
            else:
                rpc_callable(request, metadata=authed_meta.to_grpc_metadata(), timeout=20.0)
        except grpc.RpcError as exc:
            try:
                err_code = exc.code().name
            except Exception:
                err_code = "UNKNOWN"
        return (time.perf_counter() - start) * 1000.0, err_code  # ms; streaming rows = stream-open

    # All 262 RPCs are measured. Unary = full round-trip; streaming = stream-open
    # latency (initiate + push request, no response drain), so a passive subscription
    # never blocks to the deadline (that 20 s drain is what produced the bogus 272 ms).
    samples = []
    for client_cls, method in ALL_RPCS:
        client = clients[client_cls]
        streaming = method.client_streaming or method.server_streaming
        kind = "stream_open" if streaming else RPC_OPERATION_KIND.get(rpc_path(method), "read_only")
        n = 1 if streaming else iters_for(kind)
        time_one(client, method)  # warm-up
        runs = [time_one(client, method) for _ in range(n)]
        durs = sorted(d for d, _ in runs)
        # Last observed non-OK status code marks the RPC failed (mirrors Go's lastErrCode).
        err_code = "OK"
        for _, code in runs:
            if code != "OK":
                err_code = code
        def pct(p):
            return durs[min(len(durs) - 1, (p * (len(durs) - 1)) // 100)]
        samples.append({
            "service": client_cls._SERVICE_FULL.split(".")[-1],
            "rpc": method.name, "kind": kind, "iters": n, "err": err_code,
            "p50": pct(50), "p99": pct(99), "mean": sum(durs) / len(durs),
        })
    for c in clients.values():
        c.close()

    svc = {}
    for s in samples:
        svc.setdefault(s["service"], []).append(s["mean"])
    lines = ["# UDB SDK Live Perf — Python (localhost)", "",
             f"RPCs measured: {len(samples)}", "",
             "Unary = full request/response round-trip. Streaming rows (kind=stream_open) report "
             "stream-open latency (initiate + push request, no response drain), NOT first-message "
             "latency — a subscription stream emits only on events.", "",
             "## Per-service mean latency", "", "| Service | RPCs | mean ms |", "|---|--:|--:|"]
    for name in sorted(svc, key=lambda k: -sum(svc[k]) / len(svc[k])):
        lines.append(f"| {name} | {len(svc[name])} | {sum(svc[name]) / len(svc[name]):.2f} |")
    # Failures subsection: every RPC whose last iteration returned a non-OK gRPC status.
    # A failing RPC is a FAILURE with its code, never a silent latency sample.
    failed = [s for s in samples if s["err"] != "OK"]
    lines += ["", f"## Failures ({len(failed)})", ""]
    if not failed:
        lines.append("No RPC returned a non-OK gRPC status.")
    else:
        lines.append("These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.")
        lines += ["", "| RPC | kind | err | p99 ms | mean ms | iters |", "|---|---|---|--:|--:|--:|"]
        for s in sorted(failed, key=lambda x: (x["service"], x["rpc"])):
            lines.append(f"| {s['service']}/{s['rpc']} | {s['kind']} | {s['err']} | {s['p99']:.2f} | {s['mean']:.2f} | {s['iters']} |")
    lines += ["", "## Slowest 20 by p99", "", "| RPC | kind | err | p50 ms | p99 ms | mean ms |", "|---|---|---|--:|--:|--:|"]
    for s in sorted(samples, key=lambda x: -x["p99"])[:20]:
        lines.append(f"| {s['service']}/{s['rpc']} | {s['kind']} | {s['err']} | {s['p50']:.2f} | {s['p99']:.2f} | {s['mean']:.2f} |")
    report = "\n".join(lines) + "\n"
    with open("perf_report_python.md", "w", encoding="utf-8") as fh:
        fh.write(report)
    print(f"\nPython perf: {len(samples)} RPCs measured, {len(failed)} FAILED (non-OK gRPC status) → sdk/python/perf_report_python.md")
