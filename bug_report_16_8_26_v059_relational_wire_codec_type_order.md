# Bug report: relational wire decoding matched broad scalar types before exact values

Date: 2026-08-16
Affected release: unreleased 0.5.9 integration
Target correction: 0.5.9
Severity: high served-read data integrity

## Observed

Exact-main CI run `31911237372`, job `95076709586`, passed all 153 native
service live tests and then failed the required `Wire-codec served live tests`
step. Two served round trips corrupted values on read:

- `mysql_temporal_columns_round_trip_served_live` returned MySQL `DATETIME(6)`
  with an invented UTC offset instead of the exact zone-less microsecond value.
- `postgres_real_and_non_text_arrays_round_trip_served_live` returned a populated
  PostgreSQL `bigint[]` column as JSON null.

The wire-codec suite ended with 4 passed and 2 failed tests (exit 101), so
canonical-store conformance and the remaining integration harness were skipped.

## Root cause

Both failures were type-probe ordering errors in served serializers:

- `sqlx_row_to_json` attempted `DateTime<Utc>` before `NaiveDateTime`. SQLx can
  decode a zone-less MySQL `DATETIME` through the UTC type, causing the broker
  to stamp `+00:00` onto a value whose database type carries no zone.
- `row_value_to_json` tested scalar names such as `contains("INT8")` before its
  PostgreSQL array decoder. SQLx reports `bigint[]` as `INT8[]`, so the scalar
  branch claimed the column, its `i64` decode failed, and the error collapsed
  to JSON null before the array branch could run.

## Impact

- A served MySQL read could change the semantic shape of a stored `DATETIME`.
- A populated PostgreSQL integer array could be reported as absent/null.
- The broad scalar match made the existing non-text-array decoder unreachable
  for the affected type names, defeating the previous wire-codec guarantee.

## Required correction

- Probe `NaiveDateTime` before `DateTime<Utc>` in the shared MySQL/SQLite SQLx
  serializer so zone-less values stay zone-less and microsecond-exact.
- Classify PostgreSQL `[]` types before all scalar substring checks and decode
  one consolidated SQLx-enabled element matrix: `INT2`, `INT4`, `INT8`,
  `FLOAT4`, `FLOAT8`, `BOOL`, `UUID`, and text-compatible arrays.
- Preserve both SQL NULL arrays and NULL elements inside arrays.
- Propagate any supported or text-compatible array decoder mismatch as a typed
  read failure; only a successfully decoded SQL NULL may become JSON null.
- Do not claim `NUMERIC[]` / `DECIMAL[]`: the current SQLx dependency does not
  enable a decimal codec, so those element types remain outside this correction
  and fail closed instead of silently returning JSON null.
- Remove the later duplicate array block so the served path has one authority.

## Verification required

- Exact-main CI must run the `Native services + canonical stores (live)` job's
  `Wire-codec served live tests` command:
  `cargo test --locked --lib runtime::wire_codec_live_tests -- --nocapture --test-threads=1`.
- The PostgreSQL served regression must cover `smallint[]`, each supported
  non-text array, NULL elements, and an SQL NULL array.
- Static review must confirm no array `try_get` error is converted to JSON null.
- The MySQL served regression must retain its exact `DATETIME(6)` assertion.
- No local Cargo/build/test command is run, per operator direction.

## CI evidence

Exact-main CI run `31912423188` at commit `143e42367815a56491ee5cba42a4e209bb7ef8d8`
completed successfully. Its required native/live job `95079612432` passed the
native-service suite, the served wire-codec suite, canonical-store conformance,
and the integration harness. The overall run also passed Windows and AArch64;
there were no failed jobs or repair artifacts.
