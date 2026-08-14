# UDB v0.5.7 backup export buffers each tenant table in broker memory

Date: 2026-08-14
Status: confirmed; correction not yet implemented
Affected path: `BackupService.StartTenantBackup`

## Summary

Despite the service contract saying backup streams tenant rows, export loads an
entire tenant table into `Vec<String>`, joins a second full plaintext JSONL
string, encrypts it into another full string, converts that to bytes, and sends
the full object. One large table can exhaust broker memory, and the synchronous
30-second RPC is retryable by default with no idempotency identity.

## Confirmed served path

- Each table query uses `fetch_all` and materializes every `row_to_json` result.
- `rows.join("\n")` allocates another table-sized plaintext buffer.
- Required at-rest encryption returns a complete ciphertext string, followed by
  `into_bytes`; object storage receives the whole byte vector rather than a
  bounded stream/multipart pipeline.
- No row/byte/table-size bound or spill-to-disk path is enforced.
- StartTenantBackup advertises a 30-second default deadline and three attempts,
  but it has no idempotency key or durable STARTED operation for retry recovery.

## Consequences

- A tenant with one large table can OOM or heavily pause the control-plane broker.
- Client timeout/retry can start multiple simultaneous full-table scans and leave
  multiple partial prefixes.
- Memory demand includes several plaintext/ciphertext copies and is not reflected
  by the fixed-cost backup admission permit.

## Required correction

- Stream rows through bounded JSONL framing, incremental authenticated encryption,
  hashing, and multipart object upload with backpressure.
- Enforce per-run byte/table limits and isolate backup execution from latency-
  sensitive broker tasks.
- Make backup an asynchronous, durable, idempotent job with STARTED/progress/
  COMPLETED state and explicit cancellation/recovery.
- Abort multipart uploads and reconcile partial prefixes on every failure path.
- Add a constrained-memory live test with a table larger than the permitted
  in-memory window and a response-loss retry test.

## Verification log

- Traced allocation and object-upload boundaries in the export loop.
- No production data was mutated and no correction has yet been applied.
