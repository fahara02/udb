# Wire types: what a PostgreSQL column looks like in a record

Every read path (Select, a returned record, a transaction read) hands each column
back in exactly one JSON shape, decided by the column's SQL type. A value that
cannot be decoded is an error naming the column. It is never turned into `null`.

| SQL type | JSON shape | Example |
|---|---|---|
| `smallint`, `integer`, `bigint` | number | `42` |
| `real`, `double precision` | number | `1.5` |
| `numeric(p,s)`, `decimal` | **string**, exact, at the declared scale | `"12.50"` |
| `numeric` NaN / infinities | string | `"NaN"`, `"Infinity"`, `"-Infinity"` |
| `boolean` | boolean | `true` |
| `text`, `varchar(n)` | string | `"hello"` |
| `char(n)` | string, trailing padding removed | `"AB"` for `char(4)` |
| `uuid` | string | `"0192f0c4-…"` |
| `date` | string `YYYY-MM-DD` | `"2026-10-07"` |
| `timestamptz` | string, RFC 3339 in UTC with `Z` | `"2026-10-07T09:30:00.123Z"` |
| `timestamp` (no zone) | string `YYYY-MM-DD HH:MM:SS[.f]` | `"2026-10-07 09:30:00"` |
| `time` | string `HH:MM:SS[.f]` | `"09:30:00"` |
| `json`, `jsonb` | the JSON value itself | `{"a": 1}` |
| `bytea` | string, standard base64 | `"3q2+7w=="` |
| `inet`, `cidr`, `macaddr` | string, PostgreSQL text form | `"10.1.2.3/24"` |
| user enum | string, the label | `"ACTIVE"` |
| `geography`, `geometry` | string, hex EWKB (the same form a write accepts) | `"0101000020E6…"` |
| any array (`T[]`, enum arrays) | JSON array of the element shape, `null` for a NULL element | `["1.50", null]` |

Notes:

- `numeric` is a string so no digit is lost on the way to a client. Parse it
  with an exact decimal type, never a float.
- `bigint` is a JSON number. Values beyond 2^53 are exact on the wire; decode
  them with a JSON reader that keeps integers (Go `json.Decoder.UseNumber`,
  JavaScript `BigInt`-aware parsing), not through a float.
- Timestamps carry only as many fractional digits as needed. Compare them as
  instants, not as text; compare-and-swap does the same on the broker.
- The generated SDK clients decode every shape above into the field type of
  your proto message, so most code never sees these JSON forms.
