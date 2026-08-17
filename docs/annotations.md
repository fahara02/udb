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
│    crate v0.5.17 | protocol v1.0.0                                          │
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
| **Writing** | Put the array in the record yourself. It is not produced by the generated `…ToUDBRecord` helper. |
| **Empty vs absent** | An empty array and a missing value are different. A Go `nil` slice marshals to JSON `null`, which is SQL `NULL` — not an empty array. A `NOT NULL` array column rejects it. Send `[]` explicitly. |
| **Reading numerics** | Numbers arrive as `json.Number`, not `float64`. The decoder uses `UseNumber()` so large integers survive exactly (see `sdk/go/udbclient/entity.go`). Parse with `.Int64()`, `.Float64()`, or `strconv.ParseUint` for `uint64`, and surface the parse error rather than ignoring it. |

The last row applies to every numeric read, not only arrays — it is listed here
because arrays are where people meet it first, having hand-written the binding.

## Security Metadata

Use field and table security annotations to describe sensitive fields, tenant
ownership, output behavior, audit expectations, and retention metadata. The
same descriptor metadata feeds runtime checks, generated manifests, SDK output
views, and documentation.

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
