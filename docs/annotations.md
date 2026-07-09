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
│    crate v0.4.0 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
UDB annotations let an application describe how protobuf messages map to
storage, routing, field security, and generated SDK behavior.

## Export The Contract

```bash
udb proto export --fmt
```

Then import the annotation protos from your app schemas:

```proto
import "udb/core/common/v1/db.proto";
```

## Table And Column Mapping

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

Keep long field annotations readable:

```bash
udb proto fmt proto
udb proto fmt proto --check
```

## Compatibility

Follow normal protobuf compatibility rules:

- do not reuse field numbers;
- reserve deleted names and numbers;
- avoid incompatible type changes;
- treat table/security changes as deployment-visible changes.
