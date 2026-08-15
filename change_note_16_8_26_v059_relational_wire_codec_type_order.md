# Change note: v0.5.9 restores type-exact relational wire reads

Date: 2026-08-16
Release: 0.5.9

## Changed

- PostgreSQL array types are classified before broad scalar-name matches, so an
  SQLx `INT8[]` column reaches the array decoder instead of failing an `i64`
  scalar decode and collapsing to JSON null.
- One consolidated decoder covers SQLx-enabled `smallint[]`, `integer[]`,
  `bigint[]`, boolean, real/double, UUID, and text-compatible arrays.
- Array decoding preserves NULL elements as JSON null and preserves an SQL NULL
  array as JSON null.
- A type/decoder mismatch now returns a typed read error instead of becoming
  indistinguishable from an SQL NULL array.
- MySQL `DATETIME` probes `NaiveDateTime` before `DateTime<Utc>`, retaining the
  exact zone-less microsecond value rather than inventing a `+00:00` offset.
- The duplicate later PostgreSQL array decoder is removed. `NUMERIC[]` and
  `DECIMAL[]` remain explicitly outside the supported matrix because UDB's SQLx
  feature set does not include a decimal codec; reads fail closed instead of
  silently returning JSON null.

## Evidence

- The defects were reproduced by exact-main CI run `31911237372`, job
  `95076709586`, in the served wire-codec step after all native live tests passed.
- Existing served-path MySQL and PostgreSQL round trips remain the release proof;
  the PostgreSQL matrix now additionally asserts `INT2[]`, NULL elements, and an
  SQL NULL array.
- Corrected exact-main CI run `31912423188` at
  `143e42367815a56491ee5cba42a4e209bb7ef8d8` completed successfully. Native/live
  job `95079612432` passed native services, served wire codecs, canonical-store
  conformance, and the integration harness; Windows and AArch64 passed too.
- No local Cargo/build/test command is run, per operator direction.
