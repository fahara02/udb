# Workflow RPC-sequence contract (cross-SDK mock-transport gate)

This is the **single source of truth** for the exact, ordered RPC sequence each SDK
workflow helper (facade method) is allowed to emit. The Performance And Correctness
Guardrails (`private/masterplan/simple_client_code.md`) require every workflow helper
to declare its exact RPC sequence and fail if it grows a hidden `Get`/`List`/proof-read
or fallback round trip.

The TypeScript (`facade.test.ts`), Go (`upload_test.go` / `media_test.go`), and Python
(`test_simple_client.py` / `test_media_facade.py`) mock-transport gates each read THIS
file rather than carrying their own inline ordered list, so a drift in the contract
fails every language identically.

## Column contract

`| helper | sequence |` — col1 = the workflow helper key (`Facade.method`), col2 =
the comma-separated ordered list of emitted RPC method names (gRPC unary method short
names; the literal token `PUT` denotes the presigned-URL byte transfer, not a gRPC
call). Consumers MUST key on col1 and split col2 on `,` (trimming whitespace). The
sequence is the EXACT, complete, ordered set — any extra emitted RPC (a hidden
`GetFile`/`ListFiles`/`Select`/catalog round trip) is a guardrail violation.

| helper | sequence |
| --- | --- |
| StorageFacade.uploadFile | RegisterUpload, PUT, FinalizeUpload |
| StorageFacade.uploadFile.noUrl | RegisterUpload, FinalizeUpload |
| Entity.upsert | Upsert |
| Entity.upsert.returnRecord | Upsert |
| Entity.select | Select |

Notes:
- `StorageFacade.uploadFile` is the canonical 3-step sequence (RegisterUpload → presigned
  PUT byte transfer → FinalizeUpload). The `.noUrl` variant covers the broker returning no
  presigned URL (the PUT is skipped; still no proof read). No `GetFile`/`ListFiles`/
  `GetDownloadUrl` is ever emitted.
- `Entity.upsert` is exactly one `Upsert` — no hidden `Select`/`Get`/catalog round trip.
  `Entity.upsert.returnRecord` decodes the SAME `Upsert` response record (a `return_record`
  upsert is still ONE RPC — the record rides the response, no second read). `Entity.select`
  is exactly one `Select`.
