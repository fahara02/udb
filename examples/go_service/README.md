# Go service

Thirty nonblank lines connect an authenticated client, seed a tenant-scoped file
metadata row, compare-and-swap its filename, commit an annotated outbox event in
the same transaction, and consume that event with a durable acknowledgment.

Set the SDK's environment contract (`UDB_TARGET`, `UDB_AUTH_TARGET`,
`UDB_TENANT_ID`, `UDB_PROJECT_ID`, `UDB_PURPOSE`, and either `UDB_API_KEY` or
`UDB_BEARER`). Set `EXAMPLE_FILE_ID` to a fresh UUID, then run `go run .`.
The principal needs permission to select/upsert/update this entity, begin a
transaction, enqueue the event, subscribe to its topic, and acknowledge events.
For local development, start the broker with `udb dev up --embedded`.

The consumer logs `handled <file-id> <event-id>` and keeps running. The broker
fills the row's tenant from the verified principal; the event partition key comes
from its proto annotation. `TxScope` remembers collection errors, so a failed
`Emit` also refuses the transaction even when the callback returns another
mutation's result.

The live and embedded CI lanes build and run this exact program. Their shared
test checks its line budget, reads back the committed filename, and observes the
matching event's persisted acknowledgment through `GetCdcStatus` before stopping
the process. The `replace` directive selects the SDK from this checkout; outside
the repository, remove it and select the published SDK version.
