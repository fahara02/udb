import asyncio

from udb_client import Metadata, UdbAsyncClient


async def main() -> None:
    async with UdbAsyncClient(
        "localhost:50051",
        Metadata(
            tenant_id="tenant-1",
            purpose="ocr-write",
            correlation_id="python-ocr-example",
            scopes=("udb:write", "udb:event"),
            service_identity="example.service",
            project_id="default",
            client_catalog_version="1.0.0",
        ),
    ) as client:
        await client.upsert(
            message_type="example.processing.v1.ExtractionResult",
            record={"tenant_id": "tenant-1", "result_id": "res-1", "status": "done"},
            conflict_fields=["result_id"],
            return_record=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
