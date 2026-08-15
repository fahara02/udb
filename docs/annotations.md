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
│    crate v0.5.8 | protocol v1.0.0                                          │
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
