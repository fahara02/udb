from __future__ import annotations

import os
import time
from typing import Any

from google.protobuf.json_format import MessageToJson

from gen.acme.billing.v1 import acme_billing_v1_pb2 as acme_billing
from udb.entity.v1 import types_pb2
from udb_client import Metadata, UdbClient, decode_records, to_struct
from udb_client.models import MetadataModel

TARGET_DEFAULT = "127.0.0.1:50051"
PROJECT_ID = "default"
MESSAGE_TYPE = "acme.billing.v1.Product"
COLLECTION = "acme_products"
BUCKET = "acme-billing-documents"


def main() -> None:
    target = os.getenv("UDB_TARGET", TARGET_DEFAULT)
    show_timings = os.getenv("UDB_SHOW_TIMINGS", "false").lower() == "true"
    warmup = os.getenv("UDB_WARMUP", "true").lower() == "true"

    metadata = MetadataModel(
        tenant_id="acme-org-1",
        user_id="sdk-python-example",
        purpose="billing.example",
        correlation_id="python-acme-billing-example",
        scopes=(
            "udb:read",
            "udb:write",
            "udb:admin",
            "udb:vector:read",
            "udb:vector:write",
            "udb:object:presign",
            "udb:stream",
        ),
        service_identity="examples.python",
        project_id=PROJECT_ID,
        client_catalog_version="1.0.0",
    ).to_metadata()

    with UdbClient(target, metadata) as client:
        if warmup:
            timed("client warmup", lambda: client.warmup(), show_timings)

        run_id = time.time_ns()
        product_id = f"prod-sdk-python-crud-{run_id}"

        timed(
            "relational cleanup delete",
            lambda: client.delete(
                message_type=MESSAGE_TYPE,
                filter={"product_id": product_id},
                idempotency_key=f"python-product-cleanup-{run_id}",
            ),
            show_timings,
        )

        product = acme_billing.Product(
            product_id=product_id,
            name="SDK smoke test product",
            description="Inserted by the Python UDB SDK example",
            price_cents=32900,
            sku="SDK-PY-001",
        )
        timed(
            "relational create upsert",
            lambda: client.upsert(
                message_type=MESSAGE_TYPE,
                record=product_json(product),
                conflict_fields=("product_id",),
                return_record=True,
                idempotency_key=f"python-product-create-{run_id}",
            ),
            show_timings,
        )

        rows = timed(
            "relational create select",
            lambda: client.select(
                message_type=MESSAGE_TYPE,
                filter={"product_id": product_id},
                limit=10,
            ),
            show_timings,
        )
        if record_count(rows) != 1:
            raise RuntimeError(f"relational create/read expected 1 row, got {record_count(rows)}")
        print(f"relational create/read rows={record_count(rows)}")

        product.name = "SDK smoke test product updated"
        timed(
            "relational update upsert",
            lambda: client.upsert(
                message_type=MESSAGE_TYPE,
                record=product_json(product),
                conflict_fields=("product_id",),
                return_record=True,
                idempotency_key=f"python-product-update-{run_id}",
            ),
            show_timings,
        )
        rows = timed(
            "relational update select",
            lambda: client.select(
                message_type=MESSAGE_TYPE,
                filter={"product_id": product_id},
                limit=10,
            ),
            show_timings,
        )
        if not records_contain(rows, "SDK smoke test product updated"):
            raise RuntimeError("relational update was not visible in select response")
        print(f"relational update verified rows={record_count(rows)}")

        timed(
            "relational delete",
            lambda: client.delete(
                message_type=MESSAGE_TYPE,
                filter={"product_id": product_id},
                idempotency_key=f"python-product-delete-{run_id}",
            ),
            show_timings,
        )
        rows = timed(
            "relational delete select",
            lambda: client.select(
                message_type=MESSAGE_TYPE,
                filter={"product_id": product_id},
                limit=10,
            ),
            show_timings,
        )
        if record_count(rows) != 0:
            raise RuntimeError(f"relational delete expected 0 rows, got {record_count(rows)}")
        print(f"relational delete verified rows={record_count(rows)}")

        vector = make_vector(768)
        timed(
            "vector upsert",
            lambda: client.vector_upsert(
                COLLECTION,
                [
                    types_pb2.VectorPointMutation(
                        id="33333333-3333-4333-8333-333333333333",
                        vector=vector,
                        payload=to_struct(
                            {
                                "product_id": "prod-sdk-python-001",
                                "name": "Python SDK vector point",
                            }
                        ),
                    )
                ],
                idempotency_key="python-vector-sdk-001",
            ),
            show_timings,
        )
        vector_rows = timed(
            "vector search",
            lambda: client.vector_search(COLLECTION, vector, limit=3, with_payload=True),
            show_timings,
        )
        print(f"vector search points={len(vector_rows.points)}")

        timed(
            "object put",
            lambda: client.put_object(
                BUCKET,
                "invoices/sdk/python/smoke.txt",
                b"hello from the Python UDB SDK example\n",
                content_type="text/plain",
                idempotency_key="python-object-sdk-001",
            ),
            show_timings,
        )
        object_bytes = timed(
            "object get",
            lambda: client.get_object(BUCKET, "invoices/sdk/python/smoke.txt"),
            show_timings,
        )
        if b"hello from the Python UDB SDK example" not in object_bytes:
            raise RuntimeError("object readback did not match uploaded content")
        print(f"object readback bytes={len(object_bytes)}")

        url = timed(
            "object presign",
            lambda: client.generate_presigned_url(
                BUCKET,
                "invoices/sdk/python/smoke.txt",
                method="GET",
                ttl_seconds=300,
                content_type="text/plain",
            ),
            show_timings,
        )
        print(f"object presigned url expires_at={url.expires_at_unix}")


def product_json(product: acme_billing.Product) -> bytes:
    return MessageToJson(
        product,
        preserving_proto_field_name=True,
        sort_keys=True,
        indent=None,
    ).encode("utf-8")


def record_count(rows: types_pb2.RecordSet) -> int:
    if rows.records_json:
        return len(rows.records_json)
    return len(rows.rows)


def records_contain(rows: types_pb2.RecordSet, needle: str) -> bool:
    if any(needle in record.decode("utf-8", errors="replace") for record in rows.records_json):
        return True
    return any(needle in str(row) for row in rows.rows)


def make_vector(dimension: int) -> list[float]:
    return [float((index % 17) + 1) / 17.0 for index in range(dimension)]


def timed(label: str, operation: Any, show_timings: bool) -> Any:
    if not show_timings:
        return operation()
    start = time.perf_counter()
    try:
        return operation()
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        print(f"timing {label:<28} {elapsed_ms:.2f} ms")


if __name__ == "__main__":
    main()
