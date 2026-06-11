from __future__ import annotations

import importlib
import json
import os
import pkgutil
import time
import uuid

import grpc
import pytest
from google.protobuf import struct_pb2
from google.protobuf.message_factory import GetMessageClass

from udb.services.v1 import data_broker_pb2, data_broker_pb2_grpc
from udb.core.authn.services.v1 import core_pb2 as authn_pb2
from udb.entity.v1 import admin_pb2, blob_pb2, operation_pb2, relational_pb2, stores_pb2

from udb_client.auth import UdbAuthClient
from udb_client.generated_client import (
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
    return request


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
    auth.authenticate_bearer(login.access_token)
    refreshed = auth.refresh_token(login.refresh_token)
    assert refreshed.access_token

    authed_meta = metadata(bearer_token=login.access_token)
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

    run_live_backend_e2e(caps_stub, authed_meta)

    probed = 0
    for client_cls in SERVICE_CLIENTS:
        endpoint = target if client_cls is DataBrokerClient else auth_target
        client = client_cls(endpoint, authed_meta, timeout=2.0)
        descriptor = service_descriptor(client_cls)
        try:
            for method in descriptor.methods:
                label = f"{client_cls._SERVICE_FULL}/{method.name}"
                request = default_request(method, authed_meta)
                rpc = getattr(client.stub, method.name)
                try:
                    if method.client_streaming:
                        iterator = rpc(iter([request]), metadata=authed_meta.to_grpc_metadata(), timeout=2.0)
                        if method.server_streaming:
                            drain_stream(label, iterator)
                    elif method.server_streaming:
                        drain_stream(label, rpc(request, metadata=authed_meta.to_grpc_metadata(), timeout=2.0))
                    else:
                        rpc(request, metadata=authed_meta.to_grpc_metadata(), timeout=2.0)
                except grpc.RpcError as exc:
                    assert_not_mount_failure(label, exc)
                probed += 1
        finally:
            client.close()
    assert probed == 262
