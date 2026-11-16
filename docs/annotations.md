# Proto Annotations


```text
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│    ██    ██  ██████   ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│    ██    ██  ██   ██  ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│     ██████   ██████   ██████                                               │
│                                                                            │
│    UNIVERSAL DATA BROKER                                                   │
│    gRPC data plane | native control plane | tenant/project scope guard     │
│                                                                            │
│    crate v0.6.22 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This guide is for developers defining the data model their app stores in UDB.

You describe that model in protobuf (the `.proto` files that already define your
messages). UDB *annotations* are small tags you attach to those messages and
fields to say how each one maps to a real database: which table and column it
becomes, which field carries the tenant, which fields are sensitive, and how the
generated SDKs should behave. You write the annotation once; UDB uses it to build
the database schema, route requests at runtime, and shape the SDK output.

## Export The Contract

First, get the annotation definitions so your `.proto` files can reference them:

```bash
udb proto export --fmt
```

Then import the annotation protos from your app schemas:

```proto
import "udb/core/common/v1/db.proto";
```

## Table And Column Mapping

The `table` option turns a message into a database table; the `column` option on
each field turns that field into a column. This example maps a `Customer` message
to a `crm.customers` table, marks `customer_id` as the primary key, and tags
`tenant_id` as the tenant column so UDB scopes every row to the right tenant:

```proto
message Customer {
  option (udb.core.common.v1.table) = {
    schema_name: "crm"
    table_name: "customers"
    is_table: true
  };

  string customer_id = 1 [(udb.core.common.v1.column) = {
    column_name: "customer_id"
    sql_type: "text"
    primary_key: true
  }];

  string tenant_id = 2 [(udb.core.common.v1.column) = {
    column_name: "tenant_id"
    sql_type: "text"
    tenant_column: true
    not_null: true
  }];

  string email = 3 [(udb.core.common.v1.column) = {
    column_name: "email"
    sql_type: "text"
  }];
}
```

UDB parses the message into a catalog manifest, generates backend artifacts,
and uses the manifest at runtime to route broker requests.

## Repeated Fields And Arrays

A `repeated` field maps to a SQL array (`repeated float` over `REAL[]`,
`repeated string` over `TEXT[]`). The mapping works, but there is one thing to
know before you use it:

**`udb sdk generate` emits no marshalling for array columns.** The generator
skips them deliberately, alongside injected audit columns — so unlike every
other column, you write the conversion yourself. Nothing tells you this at
generate time; you find out when a write does not round-trip.

What that means in practice, for the Go SDK:

| | |
|---|---|
| **Writing** | Put the array in the record yourself. It is not produced by the generated `…ToUDBRecord` helper, which returns `(map[string]any, error)`. |
| **Empty vs absent** | An empty array and a missing value are different. A Go `nil` slice marshals to JSON `null`, which is SQL `NULL` — not an empty array. A `NOT NULL` array column rejects it. Send `[]` explicitly. |
| **Reading numerics** | Numbers arrive as `json.Number`, not `float64`. The decoder uses `UseNumber()` so large integers survive exactly (see `sdk/go/udbclient/entity.go`). Parse with `.Int64()`, `.Float64()`, or `strconv.ParseUint` for `uint64`, and surface the parse error rather than ignoring it. |

The last row applies to every numeric read, not only arrays — it is listed here
because arrays are where people meet it first, having hand-written the binding.

## Security Metadata

Use field and table security annotations to describe sensitive fields, tenant
ownership, output behavior, audit expectations, and retention metadata. The
same descriptor metadata feeds runtime checks, generated manifests, SDK output
views, and documentation.

## Projecting Only Some Rows

A secondary store (a `vector_store`, `document_store`, `graph_store` or
`data_store`) normally receives every row of the table. Often you want only
some of them. A terminology table, for example, keeps every version of every
concept, but semantic search should only see the ones that are published and
current. Set `projection_filter` on the store:

```proto
message Concept {
  option (udb.core.common.v1.table) = {
    schema_name: "terms" table_name: "concepts" is_table: true
  };
  option (udb.core.common.v1.vector_store) = {
    backend: VECTOR_BACKEND_QDRANT
    collection_name: "concepts"
    dimension: 768
    projection_filter: "state = 'PUBLISHED' AND superseded_at IS NULL"
  };
  // ... id, state, superseded_at, embedding ...
}
```

On a `data_store`, `projection_filter` is a typed field too (or the
`projection_filter` key of its `options` map, but not both).

What UDB then does with each write:

- A row that matches is projected as usual.
- A row that does not match is not written to the store. If the store already
  holds it (the row was PUBLISHED and is now RETIRED, or `superseded_at` was
  just set), it is removed.
- A delete still deletes.
- Backfill, replay and `udb projection status|backfill` use the same filter:
  a row outside the filter that is missing from the store is in sync, and one
  the store still holds is reported as `present_outside_filter` and removed by
  the repair.
- Hybrid search over a filtered Qdrant collection (`fts_columns`) applies the
  filter to its Postgres full-text leg as well.

The filter is a small expression over the entity's own columns, compared with
literals:

| You can write | Example |
|---|---|
| comparisons `=`, `<>` (or `!=`), `<`, `<=`, `>`, `>=` | `priority >= 3` |
| `IS NULL`, `IS NOT NULL` | `superseded_at IS NULL` |
| `IN (...)`, `NOT IN (...)` | `state IN ('PUBLISHED', 'REVIEWED')` |
| `AND`, `OR`, `NOT`, parentheses | `NOT (archived = TRUE OR priority < 1)` |
| literals: `'text'` (`''` for a quote), numbers, `TRUE`, `FALSE` | `title <> 'it''s'` |

Columns can be named by proto field or column name. Nothing else is accepted:
no functions, no arithmetic, no comparing two columns, and no `= NULL` (write
`IS NULL`). Text, uuid, number and boolean columns can be compared; date/time,
JSON, array, bytes and vector columns support only `IS [NOT] NULL`; encrypted
columns cannot be used at all, because the projection worker sees only their
ciphertext. A NULL column never matches a comparison (SQL rules: `state <>
'DRAFT'` is not true when `state` is NULL).

UDB checks the filter when it builds the catalog. An unknown column, a literal
of the wrong type (`priority = 'high'` on an integer column, a value outside a
column's `enum_values`), a syntax error, or a filter on a ClickHouse projection
(append-only, so it cannot remove a row that leaves the filter) is a catalog
error naming the entity, the store and the problem. The catalog manifest
(`GetCatalogManifest`) shows each projection's filter in its options, rewritten
with physical column names.

At runtime, a projected row that lacks a filter column or holds a value of
another type fails its projection task (visible in the task's error and the
projection metrics) instead of being projected or dropped by guesswork.

## Formatting

Annotations get long. These commands keep them tidy — the first reformats your
proto files in place, the second checks formatting without changing anything
(handy in CI):

```bash
udb proto fmt proto
udb proto fmt proto --check
```

## Compatibility

Because these are protobuf messages, the usual protobuf compatibility rules
apply — and because they also define your database schema, changing them changes
your deployment. Follow these rules:

- do not reuse field numbers;
- reserve deleted names and numbers;
- avoid incompatible type changes;
- treat table/security changes as deployment-visible changes.
