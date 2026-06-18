// Storage upload + streaming download (UDB 0.3.6).
//
// Connect to a UDB broker, log in, upload a file through the native
// StorageService (RegisterUpload -> presigned HTTP PUT -> FinalizeUpload), then
// read the same bytes back via the new 0.3.6 server-streaming
// `StorageService.DownloadFile` RPC.
//
// Run (after building or via ts-node):
//   UDB_TARGET=localhost:50051 \
//   UDB_TENANT=acme \
//   UDB_USER=admin UDB_PASS=secret \
//   node dist/examples/storage_upload_download.js

import { UdbProject } from "@udb_plus/sdk/project";

async function main(): Promise<void> {
  const udb = await UdbProject.connect({
    target: process.env.UDB_TARGET ?? "localhost:50051",
    tenantId: process.env.UDB_TENANT ?? "acme",
    purpose: "storage.example",
    scopes: ["udb:read", "udb:write"],
  });

  try {
    // Password login. `loginAndAdoptTenant` also authenticates the freshly
    // minted bearer and adopts the broker-verified canonical tenant id.
    await udb.loginAndAdoptTenant({
      username: process.env.UDB_USER ?? "admin",
      password: process.env.UDB_PASS ?? "secret",
    });

    // Upload bytes. uploadFile does RegisterUpload -> HTTP PUT to the presigned
    // upload_url -> FinalizeUpload and returns the FinalizeUpload response.
    const payload = Buffer.from("hello from the UDB TypeScript SDK\n", "utf8");
    const finalized: any = await udb.storage.uploadFile("greeting.txt", payload, {
      contentType: "text/plain",
      fileType: "document",
    });
    const fileId: string =
      finalized?.file_id ?? finalized?.file?.file_id ?? "";
    console.log("uploaded file_id:", fileId);

    // Streaming download (0.3.6): pull the raw bytes back over the
    // server-streaming DownloadFile RPC and reassemble into a Uint8Array. Use
    // this when presigned HTTP is unavailable; otherwise prefer the presigned
    // default (`udb.storage.downloadFile(fileId)` -> GetDownloadUrl).
    const bytes: Uint8Array = await udb.storage.downloadFileBytes(fileId);
    console.log("downloaded bytes:", bytes.length);
    console.log("content:", Buffer.from(bytes).toString("utf8"));

    // Equivalent through the canonical accessor with the streaming opt-in:
    //   const bytes = await udb.storage.downloadFile(fileId, { stream: true });
    // The default (no `stream`) instead mints a presigned download URL:
    //   const { download_url } = await udb.storage.downloadFile(fileId);
  } finally {
    udb.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
