# Migrating from a hand-written udb wrapper

Before 0.5.31 most services kept their own layer between their code and udb:
a key exchange loop, a proto-to-record codec, CAS key flattening, tenant
stamping, an outbox envelope builder, a consumer cursor loop, error-text
matching, and a fake broker for tests. The Go SDK now provides each of these,
and the broker now does the parts that belonged on the server. This guide maps
each piece to its replacement so the wrapper can be deleted.

Everything below is in `github.com/fahara02/udb/sdk/go/udbclient` unless noted.

## Connecting

| Wrapper code | Replacement |
|---|---|
| Read `UDB_*` env vars and validate them one by one | `udbclient.ConfigFromEnv("UDB_")` reports every missing or invalid variable in one error |
| Exchange the API key for a bearer, clear the key, re-exchange before expiry | `udbclient.Connect` with `Credentials{APIKey: key}` exchanges it at connect and refreshes it in the background |
| Check the principal's tenant, project, service identity and scopes after connecting | `u.Verify(udbclient.Expect{...})`, or pass `Expect` to `ConnectFromEnv` |
| Readiness probe built on an admin RPC | `u.Ready(ctx)`: the gRPC health check plus the credential state; no admin scope needed |
| Re-login when a refresh token stops working | `ConnectEnterprise` logs in again on its own |
| One client per tenant, with locking and staleness rules | `udbclient.NewAPIKeyTenantPool(base, keys)` / `NewTenantSessionPool(connect)` |
| A context that carries an end user's bearer | `u.AsUser(ctx, bearer)` |

```go
u, err := udbclient.ConnectFromEnv(ctx, "UDB_", udbclient.Expect{
	ServiceIdentity: "svc_notes",
	RequiredScopes:  []string{"udb:read", "udb:write"},
})
```

## Reading and writing rows

| Wrapper code | Replacement |
|---|---|
| `EncodeRecord` / `DecodeRecord` driven by the column annotations | `udbclient.EncodeRecord` / `DecodeRecord` (same rules; enums in text columns use udb's short tokens both ways) |
| Annotation readers (`PrimaryKeys`, `Topic`, `OwnerColumns`, `MaxLen`, ...) | `udbclient.PrimaryKeys`, `Topic`, `PartitionKeyField`, `EventType`, `OwnerColumns`, `TenantColumn`, `ExportEligible`, `RetentionClass`, `SoftDelete`, `MaxLen`, `ApplyColumnDefaults` |
| A store type with `Get`/`Select`/`Upsert`/`Delete` over `map[string]any` | `udbclient.TableOf[*pb.Entity](u)`, or the generated `<Entity>Table(u)` |
| Flattening `$and` groups into a primary-key equality for CAS | `RowKey{...}` or the generated `<Entity>Key{...}.Row()`; the broker also accepts nested equalities |
| Stamping `tenant_id` on every filter and record | Nothing. The broker fills the caller's verified tenant in, and refuses a different one with `UDB_TENANT_MISMATCH` |
| Read the whole row, merge, upsert (to avoid NOT NULL failures) | `table.Patch(ctx, key, udbclient.Record{"status": "DONE"})` |
| Head-token CAS with a conflict retry loop | `table.UpdateIf(ctx, next, expected)` or `table.UpdateWithRetry(ctx, key, 5, change)` |
| Hand-written page walkers, `Limit: 2` cardinality checks, `len(rows)==0` checks | `table.SelectAll`, `table.Get` (returns `ErrNotFound`), `TablePage.HasMore`, `SelectOptions{IncludeTotal: true}` |
| Sorting results in memory because the order varied | Reads are ordered by your sort, then the primary key, every time |
| Forcing reads to the primary after a write | Reads through the same client carry the last write's fence for 30 seconds |

```go
notes := udbclient.TableOf[*notesv1.Note](u)
n, err := notes.Get(ctx, udbclient.RowKey{"note_id": id})
if errors.Is(err, udbclient.ErrNotFound) { /* ... */ }

_, err = notes.UpdateWithRetry(ctx, udbclient.RowKey{"note_id": id}, 5, func(n *notesv1.Note) (*notesv1.Note, error) {
	n.Title = strings.TrimSpace(n.Title)
	return n, nil
})
```

## Transactions and events

| Wrapper code | Replacement |
|---|---|
| Collect mutations, drive `BeginTx`, check for `COMMITTED`, guess conflicts from the message | `u.Tx(ctx, func(tx *udbclient.TxScope) error { ... })`; a lost CAS is `ErrConflict` |
| Build the outbox envelope (`event_id`, `event_type`, `document_id`, `correlation_id`, `payload`) | `tx.Emit(event)` builds it from the event message's `message_event_contract` |
| A consumer loop: load cursor, subscribe, handle, CAS-commit the cursor, reconnect, refresh credentials | `udbclient.Consume[*eventsv1.NoteCreated](ctx, u, "search-indexer", "notes.note.created.v1", handle)`; the broker stores the cursor |

```go
err := u.Tx(ctx, func(tx *udbclient.TxScope) error {
	if err := tx.UpdateIf(next, udbclient.Record{"version": prev.Version}); err != nil {
		return err
	}
	return tx.Emit(&notesv1.NoteUpdated{NoteId: next.NoteId, TenantId: next.TenantId})
})
```

## Errors

Replace every `strings.Contains(err.Error(), ...)` with the closed error code:

| Wrapper code | Replacement |
|---|---|
| `"duplicate"`, `"23505"`, `"uq_"` | `udbclient.IsUnique(err, "uq_active_trip")` |
| `"not found"`, `"no rows"` | `udbclient.IsNotFound(err)` |
| `"precondition"`, `"conflict"`, `"expected"` | `udbclient.IsConflict(err)` |
| `"permission denied"`, guessing which scope | `udbclient.MissingScope(err)`, `udbclient.Inspect(err).Missing` |
| Anything else | `udbclient.Inspect(err)` returns `Code`, `Reason` (see [error-reasons.md](error-reasons.md)), `Constraint`, `Column`, `FixHint`, `RetryAfter` |

## Files, flags, sessions

| Wrapper code | Replacement |
|---|---|
| Upload, presign, finalize, stat, read, delete | `u.Storage.UploadFile`, `RegisterUpload`, `FinalizeUpload`, `GetFile`, `DownloadFileBytes`, `DeleteFileMode` |
| A native-user login just to reach Storage | Not needed: Storage accepts service-account bearers |
| Feature-flag evaluation | `u.Flags().Enabled(ctx, "new-editor", attrs)` |
| Session validation | `u.Auth.ValidateSession(ctx, token)` returns the public session id, user, tenant and expiry |

## Tests

Delete the fake broker. `github.com/fahara02/udb/sdk/go/udbtest` serves the
same RPCs in memory with the broker's rules (tenant fill and refusal, primary
keys, conditional writes, `require_affected`, the filter grammar, paging,
transactions, outbox events, durable consumers, typed refusal reasons):

```go
fake := udbtest.New(t, &notesv1.Note{})
u := fake.Client(t, tenantID)
// ... exercise the service with u ...
events := fake.Events() // outbox events committed by transactions
```

The same contract suite (`udbtest/conformance`) runs against the fake in unit
tests and against a live broker in CI, so the fake cannot quietly drift.

## Server behaviour you can stop working around

- NUMERIC columns read back as exact decimal strings (they used to read as NULL)
  and write without passing through a float.
- A value that cannot be decoded is an error naming the column, never a NULL.
- A bare Select (no `fields`) of a table with PII columns no longer needs
  `udb:pii:read`; it never returned those columns. Writing back a redacted
  placeholder is refused (`UDB_REDACTED_VALUE_WRITE`).
- `$nin`, `$between`, `$not` and `{"$is_null": false}` are supported.
- Update and Delete accept `require_affected`; a single-row write that matches
  nothing fails with `UDB_NO_ROWS_AFFECTED` instead of reporting success.
- Every response carries `x-udb-version`; set `UDB_EXPECTED_VERSION` on the
  broker to refuse a mismatched build at boot.
- A new entity needs no broker rebuild: `udb catalog stage` then
  `udb catalog activate`.
