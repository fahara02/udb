# Error reasons

<!-- Generated from src/runtime/error_reasons.rs; edit the table there. -->

Every refusal the broker sends carries a typed `ErrorDetail` (the `udb-error-detail-bin` trailer). Its `reason` is one of the codes below. A code is never renamed once shipped: branch on it, never on the message text. The SDKs expose it as `Reason()` / `reason`.

| Reason | gRPC code | Kind | When | Fix |
|---|---|---|---|---|
| `UDB_CAS_CONFLICT` | FailedPrecondition | ERROR_KIND_CONFLICT | A compare-and-swap precondition did not hold: the row changed since it was read. | Read the row again, reapply your change to the fresh value, and retry. |
| `UDB_CAS_ROW_MISSING` | FailedPrecondition | ERROR_KIND_CONFLICT | A compare-and-swap named a row that does not exist (or is not visible to the caller). | Create the row with a plain write, or check the key. |
| `UDB_CAS_KEY_NOT_PK` | FailedPrecondition | ERROR_KIND_VALIDATION | A conditional write's filter is not an equality on every primary-key column or on every column of a declared unique key. | Address the row by its full primary key or by one complete unique key; the generated clients do this from the entity's Key type. |
| `UDB_REVISION_CONFLICT` | FailedPrecondition | ERROR_KIND_CONFLICT | An expected_revision did not match the row's current revision. | Read the row again to get its current revision and retry. |
| `UDB_IDEMPOTENCY_REUSE` | FailedPrecondition | ERROR_KIND_CONFLICT | An idempotency key was already used with different authoritative inputs; the new write was refused. | Reuse a key only to retry identical inputs; use a new key for a different write. |
| `UDB_NO_ROWS_AFFECTED` | NotFound | ERROR_KIND_NOT_FOUND | require_affected was set and the write matched a different number of rows; nothing was changed. | Check the filter or key; read the row first if it may have been deleted. |
| `UDB_UNIQUE_VIOLATION` | AlreadyExists | ERROR_KIND_UNIQUE | The write would duplicate a value a unique constraint covers; ErrorDetail.constraint names it. | Use the existing row (read it by that unique key) or change the conflicting value. |
| `UDB_NOT_NULL_VIOLATION` | InvalidArgument | ERROR_KIND_NOT_NULL | A NOT NULL column received no value; ErrorDetail.column names it. | Send a value for the column. |
| `UDB_UPSERT_INCOMPLETE_ROW` | InvalidArgument | ERROR_KIND_NOT_NULL | An Upsert inserts a full row, and the record left out a NOT NULL column that has no default. | Send the whole row to Upsert, or use Update (the generated clients' Patch) to change only some columns of an existing row. |
| `UDB_FOREIGN_KEY_VIOLATION` | FailedPrecondition | ERROR_KIND_FOREIGN_KEY | The write references a row that does not exist, or deletes a row others still reference. | Create the referenced row first, or remove the references before deleting. |
| `UDB_TYPE_MISMATCH` | InvalidArgument | ERROR_KIND_VALIDATION | A value does not fit its column's type; ErrorDetail.column names it. | Send the value in the column's wire form (see docs/wire-types.md). |
| `UDB_DECODE_FAILED` | Internal | ERROR_KIND_INTERNAL | A stored value could not be decoded into its wire form; ErrorDetail.column names it. | Report this with the column's SQL type; it is a broker defect, not a request error. |
| `UDB_UNSUPPORTED_FILTER_OPERATOR` | InvalidArgument | ERROR_KIND_VALIDATION | A filter used an operator the broker does not support. | Use one of $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $between, $like, $ilike, $is_null, $not_null, $not, $and, $or. |
| `UDB_NULL_COMPARISON` | InvalidArgument | ERROR_KIND_VALIDATION | A filter compared a column with NULL using equality, which matches no row. | Use {"$is_null": true} (or $not_null) to match missing values. |
| `UDB_TENANT_MISMATCH` | PermissionDenied | ERROR_KIND_PERMISSION | The request named a tenant other than the caller's verified tenant. | Leave the tenant out (the broker uses the caller's tenant) or authenticate as the right tenant. |
| `UDB_TABLE_NOT_TENANT_SCOPED` | InvalidArgument | ERROR_KIND_VALIDATION | A filter named a tenant column on a table that has none. | Drop the tenant filter for this entity; it is not tenant-scoped. |
| `UDB_SCOPE_MISSING` | PermissionDenied | ERROR_KIND_PERMISSION | The caller lacks a scope the operation needs; ErrorDetail.missing names it. | Add the scope to the service account's grant and request it when connecting. |
| `UDB_POLICY_DENIED` | PermissionDenied | ERROR_KIND_PERMISSION | No policy rule allows this action for the caller; ErrorDetail.missing names the rule that would. | Add the rule (`udb authz explain` shows the closest candidate and the attribute that failed). |
| `UDB_REDACTED_VALUE_WRITE` | InvalidArgument | ERROR_KIND_REDACTED | A write tried to store the redaction placeholder in a protected column, which would erase the real value. | Read the row with the PII read scope before writing it back, or leave the column out of the write. |
| `UDB_RATE_LIMITED` | ResourceExhausted | ERROR_KIND_RATE_LIMITED | A rate limit bucket is exhausted; ErrorDetail.missing describes the bucket and retry_after_ms when to retry. | Retry after retry_after_ms, batch lookups with $in, or raise the bucket's limit. |
| `UDB_UNKNOWN_MESSAGE_TYPE` | InvalidArgument | ERROR_KIND_SCHEMA | The message type is not in the project's active catalog. | Stage and activate a catalog that contains the entity (`udb catalog stage` / `udb catalog activate`). |
| `UDB_GRANT_OWNED_BY_OTHER` | FailedPrecondition | ERROR_KIND_CONFLICT | The service identity is already granted to another service account; a grant is never moved implicitly. | Use a distinct service identity, or move the grant on purpose with `udb auth grant transfer` (or `udb identity apply --allow-transfer`). |
| `UDB_ENVELOPE_VERSION_UNSUPPORTED` | FailedPrecondition | ERROR_KIND_SCHEMA | An event's envelope_version is newer than the consumer understands. | Upgrade the consumer's SDK to the broker's version. |
| `UDB_PASSWORD_SETUP_REQUIRED` | FailedPrecondition | ERROR_KIND_VALIDATION | The invited account has no user-selected password yet and cannot start a login session. | Complete ResetPassword using the invitation code delivered to the account email, then log in. |
| `UDB_POLICY_WRONG_SURFACE` | FailedPrecondition | ERROR_KIND_VALIDATION | DataBroker.PutPolicy writes the legacy ABAC table, which does not authorize requests. | Use AuthzService.PutAuthzPolicy or udb policy apply; UDB_ALLOW_LEGACY_PUT_POLICY=true is a temporary migration override. |
