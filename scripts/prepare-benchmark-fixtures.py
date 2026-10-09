#!/usr/bin/env python3
"""Prepare disposable, served benchmark inputs after each isolated CI DB reset.

Completion and acknowledgement consume state. Give them independent fixtures
instead of timing a nonexistent upload or a workflow changed by SignalWorkflow.
The JSON contains only reference IDs and ETags, never credentials or signed URLs.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
import uuid
from dataclasses import replace
from pathlib import Path

import grpc

from bootstrap_benchmark_project_catalog import required_env
from udb.core.workflow.services.v1 import workflow_service_pb2 as workflow_pb
from udb.core.workflow.services.v1 import workflow_service_pb2_grpc as workflow_grpc
from udb.entity.v1 import admin_pb2, blob_pb2, cdc_pb2, outbox_pb2
from udb.services.v1 import data_broker_pb2_grpc
from udb_client.auth import UdbAuthClient
from udb_client.metadata import Metadata


def main() -> int:
    output = Path(required_env("UDB_BENCH_FIXTURES"))
    metadata = Metadata(
        tenant_id=required_env("UDB_LIVE_TENANT"),
        project_id=required_env("UDB_LIVE_PROJECT"),
        purpose="ci.benchmark.fixtures",
        correlation_id=str(uuid.uuid4()),
        client_catalog_version="",
    )
    with UdbAuthClient(required_env("UDB_AUTH_GRPC_TARGET"), metadata, timeout=15.0) as auth:
        login = auth.login(required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD"),
                           device_name="ci-benchmark-fixtures")
        principal = auth.authenticate_bearer(login.access_token).principal
        if not principal.tenant_id or principal.project_id != metadata.project_id:
            raise RuntimeError("benchmark fixture principal is not bound to the target project")
    metadata = replace(metadata, tenant_id=principal.tenant_id, bearer_token=login.access_token)
    headers = metadata.to_grpc_metadata()
    context = metadata.to_request_context()
    with grpc.insecure_channel(required_env("UDB_GRPC_TARGET")) as data_channel, \
            grpc.insecure_channel(required_env("UDB_AUTH_GRPC_TARGET")) as native_channel:
        broker = data_broker_pb2_grpc.DataBrokerStub(data_channel)
        workflow = workflow_grpc.WorkflowServiceStub(native_channel)
        # ACK measures a real retained event in this exact verified scope.
        # Open the stream first and wait for its response headers so the event
        # cannot race a fresh subscription's journal anchor.
        ack_topic = "udb.sdk.benchmark.ack"
        ack_document = str(uuid.uuid4())
        events = broker.PublishCDC(cdc_pb2.CDCSubscriptionRequest(
            context=context, topic_pattern=ack_topic), metadata=headers, timeout=30.0)
        try:
            events.initial_metadata()
            event = broker.EnqueueOutboxEvent(outbox_pb2.EnqueueOutboxEventRequest(
                context=context, topic=ack_topic, partition_key=ack_document,
                payload={
                    "event_id": str(uuid.uuid4()), "event_type": ack_topic,
                    "correlation_id": str(uuid.uuid4()), "document_id": ack_document,
                    "tenant_id": principal.tenant_id, "project_id": principal.project_id,
                }), metadata=headers, timeout=15.0)
            if not event.enqueued or not event.event_id:
                raise RuntimeError("CDC acknowledgement fixture was not enqueued")
            for envelope in events:
                if envelope.event_id != event.event_id:
                    continue
                payload = json.loads(envelope.payload_json)
                if (envelope.topic != ack_topic or payload.get("tenant_id") != principal.tenant_id
                        or payload.get("project_id") != principal.project_id):
                    raise RuntimeError("CDC acknowledgement event has a foreign scope")
                break
            else:
                raise RuntimeError("CDC acknowledgement event was not delivered")
            ack_event_id = event.event_id
        finally:
            events.cancel()
        bucket = os.getenv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
        broker.EnsureResource(admin_pb2.ResourceAdminRequest(
            context=context, backend="minio", resource_name=bucket, spec_json="{}"),
            metadata=headers, timeout=20.0)
        object_key = f"benchmark-multipart/{uuid.uuid4()}.txt"
        upload = broker.InitiateMultipartUpload(blob_pb2.MultipartUploadRequest(
            context=context, bucket=bucket, object_key=object_key,
            content_type="text/plain", part_count=1, ttl_seconds=3600),
            metadata=headers, timeout=20.0)
        if not upload.upload_id or len(upload.part_urls) != 1:
            raise RuntimeError("multipart fixture initiation returned no upload or part URL")
        data = b"UDB benchmark multipart fixture\n"
        try:
            request = urllib.request.Request(upload.part_urls[0], data=data, method="PUT",
                                             headers={"Content-Type": "text/plain"})
            with urllib.request.urlopen(request, timeout=20.0) as response:
                etag = response.headers.get("ETag", "").strip().strip('"')
                if response.status != 200 or not etag:
                    raise RuntimeError("multipart fixture PUT returned no ETag")
        except Exception:
            broker.AbortMultipartUpload(blob_pb2.AbortMultipartUploadRequest(
                context=context, bucket=bucket, object_key=object_key, upload_id=upload.upload_id),
                metadata=headers, timeout=20.0)
            raise RuntimeError("multipart fixture part upload failed") from None
        instance = workflow.StartWorkflow(workflow_pb.StartWorkflowRequest(
            tenant_id=principal.tenant_id, project_id=principal.project_id,
            workflow_type="sdk.perf.ack", total_steps=1, payload="{}", compensations="[]",
            correlation_id=str(uuid.uuid4())), metadata=headers, timeout=20.0)
        if not instance.workflow_id:
            raise RuntimeError("acknowledgement fixture returned no workflow ID")
        deadline = time.monotonic() + 30.0
        while True:
            current = workflow.GetWorkflow(workflow_pb.GetWorkflowRequest(
                tenant_id=principal.tenant_id, workflow_id=instance.workflow_id),
                metadata=headers, timeout=10.0).workflow
            if json.loads(current.payload or "{}").get("awaiting_ack_step") == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("acknowledgement fixture was not dispatched within 30 seconds")
            time.sleep(0.1)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({
            "schema_version": 1, "tenant_id": principal.tenant_id, "project_id": principal.project_id,
            "fixtures": {
                "multipart_bucket": bucket, "multipart_object_key": object_key,
                "multipart_upload_id": upload.upload_id, "multipart_etag": etag,
                "ack_workflow_id": instance.workflow_id,
                "cdc_ack_event_id": ack_event_id, "cdc_ack_topic": ack_topic,
            },
        }, sort_keys=True) + "\n", encoding="utf-8")
    print("Prepared real CDC event, multipart and dispatched workflow benchmark fixtures")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
