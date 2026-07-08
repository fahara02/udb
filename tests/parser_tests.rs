use std::fs;

use serde_json::json;
use udb::{
    AnnotationParserMode, CachePolicyRequest, CacheStore, CatalogManifest, ChangeKind,
    ChangeSafety, DbExtension, DbTrigger, DeletePlanRequest, DriftSeverity, DsnGenerationConfig,
    FakeProvisioningExecutor, GenericDispatchRequest, LintInput, LintSeverity, ManifestStore,
    MaterializedView, MigrationFsmState, MigrationPlanConfig, ObjectAccessRequest,
    ObjectStreamPlanRequest, ParserConfig, ProtoColumn, ProtoColumnSecurity, ProtoDefinition,
    ProtoForeignKey, ProtoSchema, RequestContext, SelectPlanRequest, SortSpec, SqlGenerationConfig,
    StorageField, TransactionMutation, TransactionPlanRequest, UpsertPlanRequest,
    VectorSearchPlanRequest, VectorUpsertPlanRequest, build_audit_event, build_cache_policy_plan,
    build_delete_plan, build_drift_report, build_generic_dispatch_plan, build_migration_plan,
    build_object_stream_plan, build_provisioning_plan, build_select_query_plan,
    build_transaction_plan, build_upsert_plan, build_vector_search_plan, build_vector_upsert_plan,
    evaluate_object_access, generate_bootstrap_sql, generate_unified_dsn_catalog, lint_catalog,
    migrate_manifest_to_current, parse_ast_source, parse_directory, parse_file, parse_file_report,
    parse_proto_source, parse_unified_dsn, plan_repairs, redact_dsn, resolve_unified_dsn,
    schema_checksum,
};

const PROTO_FIXTURE: &str = r#"
syntax = "proto3";
package example.docify.intake.entity.v1;

import "example/examplecore/common/v1/db.proto";

message PastCorrection {
  option (example.examplecore.common.v1.table) = {
    table_name: "past_corrections"
    schema_name: "example_processing"
    migration_order: 40
    is_table: true
    enable_rls: true
    comment: "Correction memory"
  };

  option (example.examplecore.common.v1.vector_store) = {
    backend: VECTOR_BACKEND_QDRANT
    collection_name: "past_corrections"
    dimension: 1536
    distance: VECTOR_DISTANCE_COSINE
    on_disk: true
  };

  option (example.examplecore.common.v1.cache) = {
    backend: CACHE_BACKEND_REDIS
    key_pattern: "ocr:correction:{correction_id}"
    ttl_seconds: 3600
    write_through: true
    read_through: true
  };

  string correction_id = 1 [(example.examplecore.common.v1.column) = {
    column_name: "correction_id"
    sql_type: "UUID"
    not_null: true
    primary_key: true
    default_value: "gen_random_uuid()"
  }];

  string document_id = 2 [(example.examplecore.common.v1.column) = {
    column_name: "document_id"
    sql_type: "UUID"
    not_null: true
    foreign_key: {
      references_table: "documents"
      references_schema: "example_intake"
      references_column: "document_id"
      on_delete: REFERENTIAL_ACTION_CASCADE
      constraint_name: "fk_past_corrections_document"
    }
    index: {
      index_name: "idx_past_corrections_document_id"
      index_type: INDEX_TYPE_BTREE
    }
  }];

  string s3_artifact_uri = 3 [
    (example.examplecore.common.v1.column) = {
      column_name: "s3_artifact_uri"
      sql_type: "TEXT"
    },
    (example.examplecore.common.v1.storage) = {
      backend: STORAGE_BACKEND_MINIO
      bucket_env_key: "example_ARTIFACTS"
      key_prefix: "corrections/{tenant_id}"
      presigned_read: true
      presigned_ttl_seconds: 900
    }
  ];
}
"#;

const EXTENDED_STORE_FIXTURE: &str = r#"
syntax = "proto3";
package example.docify.rag.entity.v1;

import "example/examplecore/common/v1/db.proto";

message RagEdge {
  option (example.examplecore.common.v1.table) = {
    table_name: "rag_edges"
    schema_name: "example_rag"
    migration_order: 20
    is_table: true
  };

  option (example.examplecore.common.v1.graph_store) = {
    backend: GRAPH_BACKEND_NEO4J
    graph_name: "document_knowledge"
    node_label: "Document"
    id_field: "edge_id"
  };

  option (example.examplecore.common.v1.timeseries_store) = {
    backend: TIMESERIES_BACKEND_TIMESCALE
    database_name: "example_metrics"
    measurement_name: "rag_edge_scores"
    time_field: "created_at"
    value_fields: "score"
  };

  option (example.examplecore.common.v1.column_store) = {
    backend: COLUMN_BACKEND_CLICKHOUSE
    database_name: "example_analytics"
    table_name: "rag_edges_fact"
    partition_key: "tenant_id"
  };

  string edge_id = 1 [(example.examplecore.common.v1.column) = {
    column_name: "edge_id"
    sql_type: "UUID"
    primary_key: true
    not_null: true
  }];
  string tenant_id = 2 [(example.examplecore.common.v1.column) = {
    column_name: "tenant_id"
    sql_type: "UUID"
    not_null: true
  }];
  double score = 3 [(example.examplecore.common.v1.column) = {
    column_name: "score"
    sql_type: "DOUBLE PRECISION"
    not_null: true
  }];
}
"#;

#[test]
fn parses_example_table_and_universal_storage_options() {
    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/intake/entity/v1/correction.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, PROTO_FIXTURE).unwrap();

    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    assert_eq!(schemas.len(), 1);

    let schema = &schemas[0];
    assert_eq!(schema.message_name, "PastCorrection");
    assert_eq!(schema.schema_name, "example_processing");
    assert_eq!(schema.table_name, "past_corrections");
    assert!(schema.enable_rls);
    assert_eq!(
        schema.vector_store.as_ref().unwrap().collection_name,
        "past_corrections"
    );
    assert_eq!(schema.vector_store.as_ref().unwrap().dimension, 1536);
    assert_eq!(schema.cache.as_ref().unwrap().ttl_seconds, 3600);

    let id = schema
        .columns
        .iter()
        .find(|col| col.column_name == "correction_id")
        .unwrap();
    assert!(id.is_primary);
    assert_eq!(id.default_value, "gen_random_uuid()");

    let document_id = schema
        .columns
        .iter()
        .find(|col| col.column_name == "document_id")
        .unwrap();
    assert_eq!(
        document_id.foreign_key.as_ref().unwrap().ref_schema,
        "example_intake"
    );
    assert_eq!(document_id.indexes[0].index_type, "BTREE");

    let artifact = schema
        .columns
        .iter()
        .find(|col| col.column_name == "s3_artifact_uri")
        .unwrap();
    assert_eq!(
        artifact.storage.as_ref().unwrap().backend,
        "STORAGE_BACKEND_MINIO"
    );

    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    assert!(
        manifest
            .stores
            .iter()
            .any(|store| store.store_kind == "vector" && store.backend == "qdrant")
    );
    assert!(
        manifest
            .stores
            .iter()
            .any(|store| store.store_kind == "cache" && store.backend == "redis")
    );
    assert!(
        manifest
            .stores
            .iter()
            .any(|store| store.store_kind == "object" && store.backend == "minio")
    );
}

#[test]
fn parses_arbitrary_extension_namespace_by_default() {
    let dir = tempfile_dir();
    let proto_path = dir.join("acme/payments/v1/payment.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(
        &proto_path,
        r#"
syntax = "proto3";
package acme.payments.v1;

message PaymentEvent {
  option (acme.platform.udb.v9.table) = {
    table_name: "payment_events"
    schema_name: "payments"
    is_table: true
  };

  string event_id = 1 [(acme.platform.udb.v9.column) = {
    column_name: "event_id"
    sql_type: "UUID"
    primary_key: true
    not_null: true
  }];
}
"#,
    )
    .unwrap();

    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].schema_name, "payments");
    assert_eq!(schemas[0].table_name, "payment_events");
    assert_eq!(schemas[0].columns[0].column_name, "event_id");
}

#[test]
fn parses_arbitrary_project_example_without_project_specific_imports() {
    let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/go_arbitary_project/proto/acme/billing/v1/acme_billing_v1.proto");
    let source = fs::read_to_string(&example).unwrap();
    assert!(!source.contains("legacyproject/"));
    assert!(!source.contains("example/"));
    assert!(!source.contains("example/"));

    let schemas = parse_file(&example, &ParserConfig::default()).unwrap();
    assert!(schemas.iter().any(|schema| {
        schema.message_name == "Invoice"
            && schema.schema_name == "billing"
            && schema.table_name == "invoices"
    }));
    assert!(schemas.iter().any(|schema| {
        schema.message_name == "BillingDocument"
            && schema.generic_stores.iter().any(|store| {
                store.store_kind == "object" && store.resource_name == "acme-billing-documents"
            })
    }));
}

#[test]
fn arbitrary_project_manifest_matches_golden_shape() {
    let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/go_arbitary_project/proto/acme/billing/v1/acme_billing_v1.proto");
    let schemas = parse_file(&example, &ParserConfig::default()).unwrap();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();

    let shape = json!({
        "tables": manifest.tables.iter().map(|table| json!({
            "schema": table.schema,
            "table": table.table,
            "message": table.message_name,
            "columns": table.columns.iter().map(|column| column.column_name.clone()).collect::<Vec<_>>()
        })).collect::<Vec<_>>(),
        "stores": manifest.stores.iter().map(|store| json!({
            "kind": store.store_kind,
            "backend": store.backend,
            "resource": store.resource_name,
            "owner": format!("{}.{}", store.owner_schema, store.owner_table)
        })).collect::<Vec<_>>()
    });

    let expected = json!({
        "tables": [
            {
                "schema": "billing",
                "table": "products",
                "message": "Product",
                "columns": ["product_id", "name", "description", "price_cents", "sku"]
            },
            {
                "schema": "billing",
                "table": "invoices",
                "message": "Invoice",
                "columns": ["invoice_id", "org_id", "customer_name", "customer_email", "amount_cents", "currency", "status", "created_at", "updated_at"]
            },
            {
                "schema": "billing",
                "table": "invoice_line_items",
                "message": "InvoiceLineItem",
                "columns": ["line_item_id", "org_id", "invoice_id", "description", "unit_price", "quantity"]
            }
        ],
        "stores": [
            {
                "kind": "cache",
                "backend": "redis",
                "resource": "billing:invoice:{invoice_id}",
                "owner": "billing.invoices"
            },
            {
                "kind": "object",
                "backend": "s3",
                "resource": "acme-billing-documents",
                "owner": "public.billing_documents"
            },
            {
                "kind": "vector",
                "backend": "qdrant",
                "resource": "acme_products",
                "owner": "billing.products"
            }
        ]
    });
    assert_eq!(shape, expected);
}

#[test]
fn reports_malformed_db_annotations_with_source_locations() {
    let dir = tempfile_dir();
    let proto_path = dir.join("acme/payments/v1/broken.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(
        &proto_path,
        r#"
syntax = "proto3";
package acme.payments.v1;

message BrokenEvent {
  option (acme.udb.v1.table) {
    table_name "broken_events"
    schema_name: "payments"
    is_table: true
  };

  string event_id = 1 [(acme.udb.v1.column) = "bad"];
}
"#,
    )
    .unwrap();

    let report = parse_file_report(&proto_path, &ParserConfig::default()).unwrap();

    assert_eq!(report.schemas.len(), 1);
    assert!(!report.passed());
    assert!(report.diagnostics.iter().all(|diagnostic| {
        diagnostic.file.ends_with("broken.proto") && diagnostic.line > 0 && diagnostic.column > 0
    }));
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "db_option_missing_equal")
    );
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "db_option_colon_expected")
    );
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "db_option_expected_block")
    );
    assert!(report.diagnostics[0].to_string().contains("broken.proto:"));
}

#[test]
fn golden_fixture_covers_declared_db_proto_options() {
    let golden_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let schemas = parse_file(
        golden_dir.join("db_options_all.proto"),
        &ParserConfig::default(),
    )
    .unwrap();
    let schema = &schemas[0];

    assert_eq!(schema.table_name, "db_option_golden");
    assert_eq!(schema.partition_strategy, "PARTITION_STRATEGY_RANGE_MONTH");
    assert_eq!(schema.partition_column, "created_at");
    assert_eq!(schema.retention_days, 30);
    assert_eq!(schema.vector_store.as_ref().unwrap().hnsw_m, 16);
    assert_eq!(
        schema.vector_store.as_ref().unwrap().payload_schema_json,
        "{\"tenant_id\":\"keyword\"}"
    );
    assert_eq!(schema.cache.as_ref().unwrap().cluster_env_key, "REDIS_DSN");
    assert_eq!(
        schema.model_registry.as_ref().unwrap().storage_uri_env,
        "MODEL_STORE_URI"
    );

    let golden_id = schema
        .columns
        .iter()
        .find(|column| column.column_name == "golden_id")
        .unwrap();
    assert!(golden_id.encrypted);
    assert_eq!(
        golden_id.foreign_key.as_ref().unwrap().name,
        "fk_golden_tenant"
    );
    assert_eq!(golden_id.indexes[0].operator_class, "vector_cosine_ops");
    assert_eq!(golden_id.indexes[0].index_params[0].key, "m");

    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    let table = manifest.table("golden", "db_option_golden").unwrap();
    assert_eq!(table.indexes[0].index_params[0].value, "16");
    assert!(
        manifest
            .stores
            .iter()
            .any(|store| store.store_kind == "object" && store.resource_name == "golden_bucket")
    );
}

#[test]
fn legacy_sql_postgres_compat_fixture_matches_expected_manifest_shape() {
    let golden_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let schemas = parse_file(
        golden_dir.join("legacy_sql_postgres_compat.proto"),
        &ParserConfig::default(),
    )
    .unwrap();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    let table = manifest.table("billing", "accounts").unwrap();

    assert_eq!(table.message_name, "legacy_sqlCompatAccount");
    assert_eq!(table.primary_key, vec!["account_id"]);
    assert_eq!(table.columns.len(), 6);
    assert!(table.columns.iter().any(|column| {
        column.column_name == "created_at" && column.sql_type == "TIMESTAMPTZ" && column.not_null
    }));
    assert!(
        table
            .columns
            .iter()
            .any(|column| column.column_name == "updated_at")
    );
    assert!(
        table
            .columns
            .iter()
            .any(|column| column.column_name == "created_by")
    );
    assert!(table.enable_rls);
    assert!(table.audit_fields);
    assert!(
        table
            .indexes
            .iter()
            .any(|index| index.name == "idx_accounts_tenant_id" && index.method == "BTREE")
    );
    assert!(table.foreign_keys.iter().any(|fk| {
        fk.name == "fk_accounts_owner"
            && fk.ref_schema == "authn"
            && fk.ref_table == "users"
            && fk.on_delete == "RESTRICT"
    }));
}

#[test]
fn parses_table_level_constraints_policies_security_and_reference_shorthand() {
    let dir = tempfile_dir();
    let proto_path = dir.join("acme/payments/v1/ledger.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(
        &proto_path,
        r#"
syntax = "proto3";
package acme.payments.v1;

message LedgerEntry {
  option (acme.udb.v1.table) = {
    table_name: "ledger_entries"
    schema_name: "payments"
    is_table: true
    enable_rls: true
    force_rls: true
    audit_fields: true
    soft_delete: true
    soft_delete_column: "deleted_at"
    unlogged: true
    tablespace: "fastspace"
    extensions: {
      name: "pgcrypto"
      schema: "public"
    }
    materialized_views: {
      view_name: "ledger_entry_totals"
      schema_name: "payments"
      query: "SELECT tenant_id, count(*) AS total FROM payments.ledger_entries GROUP BY tenant_id"
      with_data: false
    }
    triggers: {
      trigger_name: "ledger_touch_updated_at"
      timing: BEFORE
      event: UPDATE
      function_name: "payments.touch_updated_at()"
      for_each: ROW
    }
    indexes: {
      index_name: "idx_ledger_tenant_status"
      composite_fields: "tenant_id"
      composite_fields: "status"
      where_clause: "deleted_at IS NULL"
    }
    foreign_keys: {
      columns: "tenant_id"
      references_schema: "authn"
      references_table: "tenants"
      references_column: "tenant_id"
      deferrable: true
      initially_deferred: true
    }
    rls_policies: {
      policy_name: "tenant_isolation"
      command: SELECT
      using: "tenant_id = current_setting('app.tenant_id')::uuid"
      permissive: true
    }
  };

  option (acme.udb.v1.security) = {
    classification_level: CONFIDENTIAL
    audit_writes: true
    encryption_required: true
  };

  string entry_id = 1 [(acme.udb.v1.column) = {
    column_name: "entry_id"
    sql_type: "UUID"
    primary_key: true
    not_null: true
  }];
  string status = 4 [(acme.udb.v1.column) = {
    column_name: "status"
    sql_type: "TEXT"
    enum_values: "PENDING"
    enum_values: "POSTED"
    collation: "C"
  }];
  string tenant_id = 2 [(acme.udb.v1.column) = {
    column_name: "tenant_id"
    sql_type: "UUID"
    not_null: true
  }];
  string account_id = 3 [
    (acme.udb.v1.column).references = "banking.accounts(account_id)",
    (acme.udb.v1.column).on_delete = REFERENTIAL_ACTION_CASCADE,
    (acme.udb.v1.security) = {
      is_pii: true
      data_class: SENSITIVE
      mask_in_logs: true
    }
  ];
}
"#,
    )
    .unwrap();

    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    let table = manifest.table("payments", "ledger_entries").unwrap();

    assert!(table.force_rls);
    assert!(table.unlogged);
    assert_eq!(table.tablespace, "fastspace");
    assert_eq!(table.soft_delete_column, "deleted_at");
    assert_eq!(table.security.classification_level, "CONFIDENTIAL");
    assert_eq!(table.extensions[0].name, "pgcrypto");
    assert_eq!(table.materialized_views[0].name, "ledger_entry_totals");
    assert_eq!(table.triggers[0].name, "ledger_touch_updated_at");
    assert_eq!(table.indexes[0].name, "idx_ledger_tenant_status");
    assert_eq!(table.foreign_keys.len(), 2);
    assert!(
        table
            .foreign_keys
            .iter()
            .any(|fk| fk.ref_schema == "banking" && fk.on_delete == "CASCADE")
    );
    assert_eq!(table.rls_policies[0].name, "tenant_isolation");
    assert!(
        table
            .columns
            .iter()
            .find(|column| column.column_name == "account_id")
            .unwrap()
            .security
            .is_pii
    );
    assert!(
        table
            .columns
            .iter()
            .any(|column| column.column_name == "created_at")
    );
    assert!(
        table
            .checks
            .iter()
            .any(|check| check.expression == "status IN ('PENDING','POSTED')")
    );

    let sql = generate_bootstrap_sql(&schemas, &SqlGenerationConfig::default()).unwrap();
    let all_sql = sql
        .iter()
        .map(|artifact| artifact.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(all_sql.contains("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\""));
    assert!(all_sql.contains("CREATE UNLOGGED TABLE IF NOT EXISTS"));
    assert!(all_sql.contains("\"status\" TEXT COLLATE \"C\""));
    assert!(all_sql.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS"));
    assert!(all_sql.contains("DROP TRIGGER IF EXISTS \"ledger_touch_updated_at\""));
    assert!(all_sql.contains("CREATE TRIGGER \"ledger_touch_updated_at\""));
    assert!(all_sql.contains("ALTER POLICY \"tenant_isolation\""));
    assert!(all_sql.contains("CREATE POLICY \"tenant_isolation\""));
    assert!(
        all_sql.contains("ALTER TABLE \"payments\".\"ledger_entries\" FORCE ROW LEVEL SECURITY")
    );
}

#[test]
fn parses_directory_and_produces_stable_checksum() {
    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/intake/entity/v1/correction.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, PROTO_FIXTURE).unwrap();

    let schemas = parse_directory(&dir, &ParserConfig::default()).unwrap();
    assert_eq!(schemas.len(), 1);
    let first = schema_checksum(&schemas).unwrap();
    let second = schema_checksum(&schemas).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.len(), 64);
}

#[test]
fn generates_unified_dsns_for_extended_store_types() {
    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/rag/entity/v1/rag_edge.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, EXTENDED_STORE_FIXTURE).unwrap();

    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let catalog = generate_unified_dsn_catalog(&schemas, &DsnGenerationConfig::default()).unwrap();

    assert!(catalog.entries.iter().any(|entry| {
        entry.store_kind == "sql"
            && entry.dsn == "udb+sql+postgres://env:UDB_SQL_DSN/example_rag/rag_edges"
    }));
    assert!(
        catalog
            .entries
            .iter()
            .any(|entry| { entry.store_kind == "graph" && entry.dsn.contains("UDB_GRAPH_DSN") })
    );
    assert!(catalog.entries.iter().any(|entry| {
        entry.store_kind == "timeseries" && entry.dsn.contains("UDB_TIMESERIES_DSN")
    }));
    assert!(
        catalog
            .entries
            .iter()
            .any(|entry| { entry.store_kind == "column" && entry.dsn.contains("UDB_COLUMN_DSN") })
    );
}

#[test]
fn parses_validates_resolves_and_redacts_unified_dsns() {
    let parsed =
        parse_unified_dsn("udb+vector+qdrant://env:UDB_VECTOR_DSN/ocr/past_corrections").unwrap();
    assert_eq!(parsed.tier, "vector");
    assert_eq!(parsed.backend, "qdrant");
    assert_eq!(parsed.env_key, "UDB_VECTOR_DSN");
    assert_eq!(parsed.resource_parts, vec!["ocr", "past_corrections"]);

    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/rag/entity/v1/rag_edge.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, EXTENDED_STORE_FIXTURE).unwrap();
    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let catalog = generate_unified_dsn_catalog(&schemas, &DsnGenerationConfig::default()).unwrap();
    let sql_entry = catalog
        .entries
        .iter()
        .find(|entry| entry.store_kind == "sql")
        .unwrap();
    assert!(sql_entry.validate().passed);

    unsafe {
        std::env::set_var(
            "UDB_SQL_DSN",
            "postgres://prime:secret@localhost:5432/example",
        );
    }
    let resolved = resolve_unified_dsn(sql_entry);
    unsafe {
        std::env::remove_var("UDB_SQL_DSN");
    }
    assert!(resolved.valid);
    assert_eq!(
        resolved.base_dsn,
        "postgres://prime:secret@localhost:5432/example"
    );
    assert_eq!(
        resolved.redacted_base_dsn,
        "postgres://prime:***@localhost:5432/example"
    );
    assert_eq!(
        redact_dsn("redis://:secret@localhost:6379/0"),
        "redis://:***@localhost:6379/0"
    );
}

#[test]
fn builds_universal_provisioning_plan_for_sql_cache_vector_and_object() {
    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/intake/entity/v1/correction.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, PROTO_FIXTURE).unwrap();

    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    let dsn_catalog =
        generate_unified_dsn_catalog(&schemas, &DsnGenerationConfig::default()).unwrap();
    let plan = build_provisioning_plan(&manifest, &dsn_catalog.entries);

    assert!(plan.actions.iter().any(|action| {
        action.store_kind == "sql"
            && action.backend == "postgres"
            && action.resource_kind == "table"
    }));
    assert!(plan.actions.iter().any(|action| {
        action.store_kind == "vector"
            && action.backend == "qdrant"
            && action.resource_kind == "vector_collection"
            && action
                .parameters
                .iter()
                .any(|param| param.key == "dimension" && param.value == "1536")
    }));
    assert!(plan.actions.iter().any(|action| {
        action.store_kind == "cache"
            && action.backend == "redis"
            && action.resource_kind == "keyspace"
    }));
    assert!(plan.actions.iter().any(|action| {
        action.store_kind == "object"
            && action.backend == "minio"
            && action.resource_kind == "bucket"
    }));
}

#[test]
fn builds_double_entry_migration_plan() {
    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/rag/entity/v1/rag_edge.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, EXTENDED_STORE_FIXTURE).unwrap();

    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let plan = build_migration_plan(None, &schemas, &MigrationPlanConfig::default()).unwrap();

    assert!(!plan.blocked);
    assert!(plan.states.contains(&MigrationFsmState::Completed));
    assert_eq!(plan.ledgers.len(), 3);
    assert!(plan.ledgers.iter().any(|ledger| {
        ledger.ledger == "resource_actions" && ledger.checksum_sha256.starts_with("sha256:")
    }));
    assert!(
        plan.sql_artifacts
            .iter()
            .any(|artifact| artifact.content.contains("CREATE TABLE IF NOT EXISTS"))
    );
    assert!(
        plan.resource_actions
            .iter()
            .any(|action| action.store_kind == "graph")
    );
    assert_eq!(plan.auto_count, plan.changes.len());
    assert_eq!(plan.blocked_count, 0);
    assert!(plan.operations_hash.starts_with("sha256:"));
    assert!(
        plan.resource_actions
            .iter()
            .all(|action| action.metric_labels.operation == "ensure")
    );
}

#[test]
fn fake_provider_executor_applies_verifies_and_repairs_plan() {
    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/rag/entity/v1/rag_edge.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, EXTENDED_STORE_FIXTURE).unwrap();

    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    let dsn_catalog =
        generate_unified_dsn_catalog(&schemas, &DsnGenerationConfig::default()).unwrap();
    let plan = build_provisioning_plan(&manifest, &dsn_catalog.entries);
    let mut executor = FakeProvisioningExecutor::default();

    let verify_before = executor.verify_plan(&plan);
    assert!(!verify_before.passed());
    let repair = executor.repair_plan(&plan);
    assert!(repair.passed());
    let verify_after = executor.verify_plan(&plan);
    assert!(verify_after.passed());
    assert_eq!(verify_after.verified, plan.actions.len());
}

#[test]
fn migrates_legacy_manifest_versions_to_current() {
    let mut manifest = CatalogManifest {
        generator_version: "1".to_string(),
        stores: vec![ManifestStore {
            store_kind: "storage".to_string(),
            backend: "minio".to_string(),
            resource_name: "artifacts".to_string(),
            ..ManifestStore::default()
        }],
        ..CatalogManifest::default()
    };

    let report = migrate_manifest_to_current(&mut manifest);
    assert!(report.migrated);
    assert_eq!(manifest.generator_version, "3");
    assert_eq!(manifest.stores[0].store_kind, "object");
}

#[test]
fn deep_store_option_diff_classifies_cache_safe_and_vector_review() {
    let dir = tempfile_dir();
    let old_path = dir.join("old/correction.proto");
    let new_path = dir.join("new/correction.proto");
    fs::create_dir_all(old_path.parent().unwrap()).unwrap();
    fs::create_dir_all(new_path.parent().unwrap()).unwrap();
    fs::write(&old_path, PROTO_FIXTURE).unwrap();
    fs::write(
        &new_path,
        PROTO_FIXTURE
            .replace("dimension: 1536", "dimension: 3072")
            .replace("ttl_seconds: 3600", "ttl_seconds: 7200"),
    )
    .unwrap();

    let old = parse_file(&old_path, &ParserConfig::default()).unwrap();
    let new = parse_file(&new_path, &ParserConfig::default()).unwrap();
    let old_manifest = CatalogManifest::from_schemas(&old).unwrap();
    let plan =
        build_migration_plan(Some(&old_manifest), &new, &MigrationPlanConfig::default()).unwrap();

    assert!(plan.changes.iter().any(|change| {
        change.kind == ChangeKind::UpdateStore
            && change.object_name == "ocr:correction:{correction_id}"
            && change.safety == ChangeSafety::SafeAuto
    }));
    assert!(plan.changes.iter().any(|change| {
        change.kind == ChangeKind::UpdateStore
            && change.object_name == "past_corrections"
            && change.safety == ChangeSafety::RequiresReview
    }));
    assert!(plan.blocked);
}

#[test]
fn broker_query_planner_routes_allowlists_and_masks_fields() {
    let mut schema = table_schema("processing", "jobs", 1);
    schema.message_name = "ProcessingJob".to_string();
    schema.columns.push(pk_col("job_id"));
    schema.columns.push(ProtoColumn {
        field_name: "tenant_id".to_string(),
        column_name: "tenant_id".to_string(),
        sql_type: "UUID".to_string(),
        field_number: 2,
        ..ProtoColumn::default()
    });
    schema.columns.push(ProtoColumn {
        field_name: "applicant_name".to_string(),
        column_name: "applicant_name".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 3,
        security: ProtoColumnSecurity {
            is_pii: true,
            mask_in_logs: true,
            ..ProtoColumnSecurity::default()
        },
        ..ProtoColumn::default()
    });
    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();

    let plan = build_select_query_plan(
        &manifest,
        &SelectPlanRequest {
            context: RequestContext {
                tenant_id: "tenant-1".to_string(),
                purpose: "processing".to_string(),
                scopes: vec!["udb:read".to_string()],
                ..RequestContext::default()
            },
            message_type: "example.processing.ProcessingJob".to_string(),
            filter: json!({"tenant_id": {"$eq": "tenant-1"}, "job_id": {"$eq": "job-1"}}),
            fields: vec!["job_id".to_string(), "applicant_name".to_string()],
            sort: vec![SortSpec {
                field: "job_id".to_string(),
                descending: true,
            }],
            ..SelectPlanRequest::default()
        },
    );
    assert!(plan.passed(), "{:?}", plan.errors);
    assert_eq!(plan.resource_uri, "sql://processing/jobs");
    assert!(plan.masked_columns.contains(&"applicant_name".to_string()));

    let raw_sql_plan = build_select_query_plan(
        &manifest,
        &SelectPlanRequest {
            context: RequestContext {
                tenant_id: "tenant-1".to_string(),
                purpose: "processing".to_string(),
                scopes: vec!["udb:read".to_string()],
                ..RequestContext::default()
            },
            message_type: "ProcessingJob".to_string(),
            filter: json!({"$raw": "1=1"}),
            ..SelectPlanRequest::default()
        },
    );
    assert!(!raw_sql_plan.passed());
    assert!(
        raw_sql_plan
            .errors
            .iter()
            .any(|error| error.contains("raw SQL"))
    );
}

#[test]
fn broker_planner_validates_vector_dimensions_object_pii_and_audit_events() {
    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/intake/entity/v1/correction.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, PROTO_FIXTURE).unwrap();
    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();

    let vector_plan = build_vector_search_plan(
        &manifest,
        &VectorSearchPlanRequest {
            context: RequestContext {
                tenant_id: "tenant-1".to_string(),
                scopes: vec!["udb:vector:read".to_string()],
                ..RequestContext::default()
            },
            collection: "past_corrections".to_string(),
            vector_dimension: 10,
            filter: json!({"tenant_id": "tenant-1"}),
            limit: 5,
        },
    );
    assert!(!vector_plan.passed());
    assert!(
        vector_plan
            .errors
            .iter()
            .any(|error| error.contains("dimension mismatch"))
    );

    let mut object_schema = table_schema("processing", "documents", 1);
    object_schema.columns.push(pk_col("document_id"));
    object_schema.columns.push(ProtoColumn {
        field_name: "artifact_uri".to_string(),
        column_name: "artifact_uri".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 2,
        security: ProtoColumnSecurity {
            is_pii: true,
            ..ProtoColumnSecurity::default()
        },
        storage: Some(StorageField {
            backend: "STORAGE_BACKEND_MINIO".to_string(),
            bucket_env_key: "PII_DOCS".to_string(),
            ..StorageField::default()
        }),
        ..ProtoColumn::default()
    });
    let object_manifest = CatalogManifest::from_schemas(&[object_schema]).unwrap();
    let denied = evaluate_object_access(
        &object_manifest,
        &ObjectAccessRequest {
            context: RequestContext {
                tenant_id: "tenant-1".to_string(),
                purpose: "processing".to_string(),
                scopes: vec!["udb:object:presign".to_string()],
                ..RequestContext::default()
            },
            bucket: "pii_docs".to_string(),
            object_key: "tenant-1/doc.pdf".to_string(),
            method: "GET".to_string(),
            presigned: true,
        },
    );
    assert!(!denied.allowed);
    assert!(denied.pii);

    let audit = build_audit_event(
        &RequestContext {
            tenant_id: "tenant-1".to_string(),
            user_id: "svc-ocr".to_string(),
            correlation_id: "corr-1".to_string(),
            purpose: "verification".to_string(),
            ..RequestContext::default()
        },
        "udb.object.presign",
        "object://pii_docs/tenant-1/doc.pdf",
        "sha256:abc",
    );
    assert_eq!(audit.tenant_id, "tenant-1");
    assert_eq!(audit.resource_uri, "object://pii_docs/tenant-1/doc.pdf");
}

#[test]
fn broker_runtime_contracts_plan_sql_cache_vector_object_and_dispatch() {
    let mut schema = table_schema("processing", "jobs", 1);
    schema.message_name = "ProcessingJob".to_string();
    schema.cache = Some(CacheStore {
        backend: "CACHE_BACKEND_REDIS".to_string(),
        key_pattern: "job:{tenant_id}:{job_id}".to_string(),
        ttl_seconds: 120,
        write_through: true,
        read_through: true,
        ..CacheStore::default()
    });
    schema.columns.push(pk_col("job_id"));
    schema.columns.push(ProtoColumn {
        field_name: "tenant_id".to_string(),
        column_name: "tenant_id".to_string(),
        sql_type: "UUID".to_string(),
        field_number: 2,
        ..ProtoColumn::default()
    });
    schema.columns.push(ProtoColumn {
        field_name: "status".to_string(),
        column_name: "status".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 3,
        ..ProtoColumn::default()
    });
    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let write_context = RequestContext {
        tenant_id: "tenant-1".to_string(),
        purpose: "processing".to_string(),
        scopes: vec!["udb:write".to_string(), "udb:stream".to_string()],
        ..RequestContext::default()
    };

    let upsert = build_upsert_plan(
        &manifest,
        &UpsertPlanRequest {
            context: write_context.clone(),
            message_type: "ProcessingJob".to_string(),
            record: json!({"job_id": "job-1", "tenant_id": "tenant-1", "status": "queued"}),
            return_record: true,
            ..UpsertPlanRequest::default()
        },
    );
    assert!(upsert.passed(), "{:?}", upsert.errors);
    assert!(upsert.sql.contains("INSERT INTO \"processing\".\"jobs\""));
    assert!(
        upsert
            .sql
            .contains("ON CONFLICT (\"job_id\") DO UPDATE SET")
    );
    assert!(upsert.sql.contains("RETURNING *"));
    assert!(upsert.cache_policy.write_through);
    assert!(upsert.cache_policy.invalidates_on_mutation);

    let delete = build_delete_plan(
        &manifest,
        &DeletePlanRequest {
            context: write_context.clone(),
            message_type: "ProcessingJob".to_string(),
            filter: json!({"tenant_id": "tenant-1", "job_id": {"$eq": "job-1"}}),
        },
    );
    assert!(delete.passed(), "{:?}", delete.errors);
    assert_eq!(delete.parameter_columns, vec!["tenant_id", "job_id"]);
    assert!(
        delete
            .sql
            .contains("DELETE FROM \"processing\".\"jobs\" WHERE")
    );

    let tx = build_transaction_plan(
        &manifest,
        &TransactionPlanRequest {
            context: write_context,
            tx_id: "tx-1".to_string(),
            commit: true,
            mutations: vec![TransactionMutation {
                operation: "delete".to_string(),
                message_type: "ProcessingJob".to_string(),
                filter: json!({"tenant_id": "tenant-1", "job_id": "job-1"}),
                ..TransactionMutation::default()
            }],
        },
    );
    assert!(tx.passed(), "{:?}", tx.errors);
    assert_eq!(tx.state, "TX_STATE_COMMITTED");

    let cache = build_cache_policy_plan(
        &manifest,
        &CachePolicyRequest {
            message_type: "ProcessingJob".to_string(),
            operation: "select".to_string(),
            bypass_read: true,
            ttl_seconds: 300,
            ..CachePolicyRequest::default()
        },
    );
    assert!(cache.passed());
    assert!(!cache.read_through);
    assert!(cache.write_through);
    assert_eq!(cache.ttl_seconds, 300);

    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/rag/entity/v1/rag_edge.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, EXTENDED_STORE_FIXTURE).unwrap();
    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let universal_manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    let dispatch = build_generic_dispatch_plan(
        &universal_manifest,
        &GenericDispatchRequest {
            context: RequestContext {
                tenant_id: "tenant-1".to_string(),
                purpose: "analytics".to_string(),
                scopes: vec!["udb:dispatch".to_string()],
                ..RequestContext::default()
            },
            store_kind: "graph".to_string(),
            resource_name: "document_knowledge".to_string(),
            operation: "upsert_node".to_string(),
        },
    );
    assert!(dispatch.passed(), "{:?}", dispatch.errors);
    assert_eq!(dispatch.backend, "neo4j");
    assert_eq!(
        dispatch.resource_uri,
        "graph://example_rag/document_knowledge"
    );
}

#[test]
fn broker_runtime_contracts_validate_vector_upserts_and_object_streams() {
    let dir = tempfile_dir();
    let proto_path = dir.join("example/docify/intake/entity/v1/correction.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(&proto_path, PROTO_FIXTURE).unwrap();
    let schemas = parse_file(&proto_path, &ParserConfig::default()).unwrap();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();

    let upsert = build_vector_upsert_plan(
        &manifest,
        &VectorUpsertPlanRequest {
            context: RequestContext {
                tenant_id: "tenant-1".to_string(),
                scopes: vec!["udb:vector:write".to_string()],
                ..RequestContext::default()
            },
            collection: "past_corrections".to_string(),
            point_dimensions: vec![1536, 10],
            payloads: vec![json!({"tenant_id": "tenant-1", "document_id": "doc-1"})],
        },
    );
    assert!(!upsert.passed());
    assert_eq!(upsert.expected_dimension, 1536);
    assert!(
        upsert
            .errors
            .iter()
            .any(|error| error.contains("dimension mismatch"))
    );
    assert!(upsert.payload_fields.contains(&"tenant_id".to_string()));

    let mut object_schema = table_schema("processing", "documents", 1);
    object_schema.columns.push(pk_col("document_id"));
    object_schema.columns.push(ProtoColumn {
        field_name: "artifact_uri".to_string(),
        column_name: "artifact_uri".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 2,
        storage: Some(StorageField {
            backend: "STORAGE_BACKEND_MINIO".to_string(),
            bucket_env_key: "DOCS".to_string(),
            presigned_read: true,
            server_side_encryption: true,
            ..StorageField::default()
        }),
        ..ProtoColumn::default()
    });
    let object_manifest = CatalogManifest::from_schemas(&[object_schema]).unwrap();
    let stream = build_object_stream_plan(
        &object_manifest,
        &ObjectStreamPlanRequest {
            context: RequestContext {
                tenant_id: "tenant-1".to_string(),
                purpose: "verification".to_string(),
                scopes: vec!["udb:stream".to_string()],
                ..RequestContext::default()
            },
            bucket: "docs".to_string(),
            object_key: "tenant-1/doc.pdf".to_string(),
            method: "PUT".to_string(),
            chunk_count: 2,
            final_chunk_seen: true,
            content_type: "application/pdf".to_string(),
        },
    );
    assert!(stream.allowed, "{:?}", stream.errors);
    assert_eq!(stream.backend, "minio");
    assert!(stream.requires_server_side_encryption);

    let presign_write = evaluate_object_access(
        &object_manifest,
        &ObjectAccessRequest {
            context: RequestContext {
                tenant_id: "tenant-1".to_string(),
                purpose: "verification".to_string(),
                scopes: vec!["udb:object:presign".to_string()],
                ..RequestContext::default()
            },
            bucket: "docs".to_string(),
            object_key: "tenant-1/doc.pdf".to_string(),
            method: "PUT".to_string(),
            presigned: true,
        },
    );
    assert!(!presign_write.allowed);
    assert!(
        presign_write
            .errors
            .iter()
            .any(|error| error.contains("presigned PUT is not enabled"))
    );
}

#[test]
fn fuzz_style_parser_and_filter_compiler_regressions_do_not_panic() {
    let dir = tempfile_dir();
    let parser_cases = [
        r#"message Broken { option (acme.udb.v1.table) { table_name "broken" is_table: true }; string id = 1 [(acme.udb.v1.column) = "bad"]; }"#,
        r#"message Broken { option (acme.udb.v1.table) = { table_name: "broken" is_table: true string id = 1; }"#,
        r#"message Broken { string id = 1 [(acme.udb.v1.column) = { column_name: "id", sql_type: "UUID" };"#,
    ];

    for (idx, body) in parser_cases.iter().enumerate() {
        let proto_path = dir.join(format!("case_{idx}.proto"));
        fs::write(
            &proto_path,
            format!("syntax = \"proto3\";\npackage acme.fuzz.v1;\n{body}"),
        )
        .unwrap();
        let _ = parse_file_report(&proto_path, &ParserConfig::default());
    }

    let mut schema = table_schema("processing", "jobs", 1);
    schema.columns.push(pk_col("job_id"));
    schema.columns.push(ProtoColumn {
        field_name: "tenant_id".to_string(),
        column_name: "tenant_id".to_string(),
        sql_type: "UUID".to_string(),
        field_number: 2,
        ..ProtoColumn::default()
    });
    schema.columns.push(ProtoColumn {
        field_name: "status".to_string(),
        column_name: "status".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 3,
        ..ProtoColumn::default()
    });
    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let filters = [
        json!({"tenant_id": {"$eq": "tenant-1"}, "$raw": "1=1"}),
        json!({"$and": [{"tenant_id": "tenant-1"}, {"unknown": "x"}]}),
        json!({"tenant_id": "tenant-1", "status": {"$in": ["queued", "done"]}}),
    ];

    for filter in filters {
        let plan = build_select_query_plan(
            &manifest,
            &SelectPlanRequest {
                context: RequestContext {
                    tenant_id: "tenant-1".to_string(),
                    purpose: "fuzz-regression".to_string(),
                    scopes: vec!["udb:read".to_string()],
                    ..RequestContext::default()
                },
                message_type: "jobs".to_string(),
                filter,
                ..SelectPlanRequest::default()
            },
        );
        assert_eq!(plan.tenant_column, "tenant_id");
    }
}

#[test]
fn builds_full_proto_file_ast_for_non_db_contracts() {
    let source = br#"
syntax = "proto3";
package example.udb.v1;

import "google/protobuf/struct.proto";

service DataBroker {
  rpc Select(SelectRequest) returns (RecordSet);
  rpc PutObject(stream Chunk) returns (stream MutationResponse);
}

message SelectRequest {
  option deprecated = false;
  string message_type = 1;
  repeated string fields = 2;
}

enum StoreKind {
  STORE_KIND_UNSPECIFIED = 0;
  STORE_KIND_VECTOR = 1;
}
"#;

    let ast = parse_ast_source(source, "inline.proto").unwrap();
    assert_eq!(ast.syntax, "proto3");
    assert_eq!(ast.package, "example.udb.v1");
    assert_eq!(ast.imports[0].path, "google/protobuf/struct.proto");
    assert!(ast.definitions.iter().any(|definition| match definition {
        ProtoDefinition::Service(service) => {
            service.name == "DataBroker"
                && service.rpcs.len() == 2
                && service.rpcs[1].client_streaming
                && service.rpcs[1].server_streaming
        }
        _ => false,
    }));
    assert!(ast.definitions.iter().any(|definition| match definition {
        ProtoDefinition::Message(message) =>
            message.name == "SelectRequest" && message.fields.len() == 2,
        _ => false,
    }));
}

#[test]
fn manifest_checksum_ignores_migration_hints_but_keeps_schema_order() {
    let mut parent = table_schema("authn", "users", 1);
    parent.columns.push(pk_col("user_id"));

    let mut child = table_schema("processing", "jobs", 2);
    child.columns.push(pk_col("job_id"));
    child.columns.push(ProtoColumn {
        field_name: "user_id".to_string(),
        column_name: "user_id".to_string(),
        sql_type: "UUID".to_string(),
        field_number: 2,
        foreign_key: Some(ProtoForeignKey {
            name: "fk_jobs_user".to_string(),
            columns: vec!["user_id".to_string()],
            ref_schema: "authn".to_string(),
            ref_table: "users".to_string(),
            ref_columns: vec!["user_id".to_string()],
            ..ProtoForeignKey::default()
        }),
        ..ProtoColumn::default()
    });

    let base = CatalogManifest::from_schemas(&[parent.clone(), child.clone()]).unwrap();
    let mut hinted_child = child;
    hinted_child.previous_table_name = "old_jobs".to_string();
    hinted_child.allow_drop = true;
    hinted_child.columns[1].previous_column_name = "actor_id".to_string();
    hinted_child.columns[1].backfill_sql = "'00000000-0000-0000-0000-000000000000'".to_string();
    let hinted = CatalogManifest::from_schemas(&[parent, hinted_child]).unwrap();

    assert_eq!(base.checksum_sha256, hinted.checksum_sha256);
    assert_eq!(base.schema_order, vec!["authn", "processing"]);
}

#[test]
fn review_required_changes_block_migration_plan() {
    let mut old = table_schema("processing", "jobs", 1);
    old.columns.push(pk_col("job_id"));

    let mut new = old.clone();
    new.columns.push(ProtoColumn {
        field_name: "status".to_string(),
        column_name: "status".to_string(),
        sql_type: "TEXT".to_string(),
        not_null: true,
        field_number: 2,
        ..ProtoColumn::default()
    });

    let old_manifest = CatalogManifest::from_schemas(&[old]).unwrap();
    let plan =
        build_migration_plan(Some(&old_manifest), &[new], &MigrationPlanConfig::default()).unwrap();

    assert!(plan.blocked);
    assert!(plan.states.contains(&MigrationFsmState::Error));
    assert!(plan.changes.iter().any(|change| {
        change.kind == ChangeKind::AddColumn
            && change.safety == ChangeSafety::RequiresReview
            && change.blocked_reason.contains("backfill_sql")
    }));
}

#[test]
fn safe_manifest_diff_generates_delta_sql_artifact() {
    let mut old = table_schema("processing", "jobs", 1);
    old.columns.push(pk_col("job_id"));

    let mut new = old.clone();
    new.columns.push(ProtoColumn {
        field_name: "status".to_string(),
        column_name: "status".to_string(),
        sql_type: "TEXT".to_string(),
        default_value: "'queued'".to_string(),
        not_null: true,
        field_number: 2,
        ..ProtoColumn::default()
    });

    let old_manifest = CatalogManifest::from_schemas(&[old]).unwrap();
    let plan =
        build_migration_plan(Some(&old_manifest), &[new], &MigrationPlanConfig::default()).unwrap();

    assert!(!plan.blocked);
    assert!(plan.sql_artifacts.iter().any(|artifact| {
        artifact.kind == "proto_delta"
            && artifact.content.contains("ADD COLUMN IF NOT EXISTS")
            && artifact
                .content
                .contains("\"status\" TEXT DEFAULT 'queued' NOT NULL")
    }));
}

#[test]
fn bootstrap_sql_backfills_columns_for_existing_partitioned_tables() {
    let mut schema = table_schema("example_mfs", "mfs_transactions", 2);
    schema.audit_fields = true;
    schema.partition_strategy = "PARTITION_STRATEGY_RANGE_MONTH".to_string();
    schema.partition_column = "created_at".to_string();
    schema.partition_interval = "MONTHLY".to_string();
    schema.columns.push(pk_col("transaction_id"));
    schema.columns.push(ProtoColumn {
        field_name: "external_transaction_id".to_string(),
        column_name: "external_transaction_id".to_string(),
        sql_type: "VARCHAR(80)".to_string(),
        unique: true,
        field_number: 2,
        ..ProtoColumn::default()
    });
    schema.indexes.push(udb::ProtoIndex {
        name: "idx_mfs_transactions_created_at".to_string(),
        columns: vec!["created_at".to_string()],
        ..udb::ProtoIndex::default()
    });
    schema.indexes.push(udb::ProtoIndex {
        name: "idx_mfs_transactions_txn_id".to_string(),
        columns: vec!["external_transaction_id".to_string()],
        unique: true,
        ..udb::ProtoIndex::default()
    });

    let artifacts = generate_bootstrap_sql(&[schema], &SqlGenerationConfig::default()).unwrap();
    let artifact = artifacts
        .iter()
        .find(|artifact| artifact.rel_path == "example_mfs/001_mfs_transactions.sql")
        .expect("mfs transaction artifact");

    assert!(
        artifact.content.contains(
            "ALTER TABLE \"example_mfs\".\"mfs_transactions\" ADD COLUMN IF NOT EXISTS \"created_at\" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL;"
        ),
        "partitioned bootstrap should repair missing partition/audit columns before indexes and pg_partman setup:\n{}",
        artifact.content
    );
    let alter_pos = artifact
        .content
        .find("ADD COLUMN IF NOT EXISTS \"created_at\"")
        .unwrap();
    let index_pos = artifact
        .content
        .find("CREATE INDEX IF NOT EXISTS \"idx_mfs_transactions_created_at\"")
        .unwrap();
    assert!(
        alter_pos < index_pos,
        "missing-column repair must run before indexes reference created_at"
    );
    assert!(
        artifact
            .content
            .contains("DROP CONSTRAINT IF EXISTS %I CASCADE"),
        "partitioned bootstrap should drop legacy unique/primary constraints that do not include created_at:\n{}",
        artifact.content
    );
    assert!(
        artifact
            .content
            .contains("_required_partition_cols TEXT[] := ARRAY['created_at']::TEXT[]")
            && artifact
                .content
                .contains("FROM unnest(_required_partition_cols)"),
        "partitioned bootstrap should evaluate legacy constraints against desired and live partition columns:\n{}",
        artifact.content
    );
    let repair_pos = artifact.content.find("FROM pg_constraint c").unwrap();
    assert!(
        repair_pos < index_pos,
        "legacy unique/PK repair must run before composite unique indexes are created"
    );
    assert!(
        artifact
            .content
            .contains("ADD CONSTRAINT %I PRIMARY KEY (%s)"),
        "partitioned bootstrap should recreate the composite primary key dynamically after dropping legacy PKs"
    );
    assert!(
        artifact
            .content
            .contains("CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I.%I USING %s (%s)"),
        "partitioned unique index creation should be dynamic so live partition keys can be appended:\n{}",
        artifact.content
    );
    assert!(
        artifact.content.contains("pg_partitioned_table p")
            && artifact
                .content
                .contains("ARRAY['\"external_transaction_id\"', '\"created_at\"']::TEXT[]"),
        "explicit unique indexes on partitioned tables must include the desired partition column and inspect live partition keys:\n{}",
        artifact.content
    );
    assert!(
        !artifact
            .content
            .contains("uidx_example_mfs_mfs_transactions_external_transaction_id"),
        "column-level unique generation should not duplicate an equivalent explicit unique index"
    );
    let partman_pos = artifact.content.find("partman.create_parent").unwrap();
    let final_repair_pos = artifact.content.rfind("FROM pg_constraint c").unwrap();
    assert!(
        final_repair_pos < partman_pos,
        "legacy unique/PK cleanup must also run immediately before pg_partman create_parent"
    );
}

#[test]
fn delta_unique_on_partitioned_table_includes_partition_column() {
    let mut old = table_schema("example_voice", "voice_sessions", 1);
    old.partition_strategy = "PARTITION_STRATEGY_RANGE_MONTH".to_string();
    old.partition_column = "started_at".to_string();
    old.partition_interval = "MONTHLY".to_string();
    old.columns.push(pk_col("session_id"));
    old.columns.push(ProtoColumn {
        field_name: "started_at".to_string(),
        column_name: "started_at".to_string(),
        sql_type: "TIMESTAMPTZ".to_string(),
        not_null: true,
        field_number: 2,
        ..ProtoColumn::default()
    });
    old.columns.push(ProtoColumn {
        field_name: "external_session_id".to_string(),
        column_name: "external_session_id".to_string(),
        sql_type: "VARCHAR(120)".to_string(),
        field_number: 3,
        ..ProtoColumn::default()
    });

    let mut new = old.clone();
    new.columns
        .iter_mut()
        .find(|column| column.column_name == "external_session_id")
        .unwrap()
        .unique = true;

    let old_manifest = CatalogManifest::from_schemas(&[old]).unwrap();
    let plan =
        build_migration_plan(Some(&old_manifest), &[new], &MigrationPlanConfig::default()).unwrap();

    assert!(!plan.blocked);
    let delta = plan
        .sql_artifacts
        .iter()
        .find(|artifact| {
            artifact.kind == "proto_delta"
                && artifact
                    .content
                    .contains("uidx_example_voice_voice_sessions_external_session_id")
        })
        .expect("unique delta artifact");
    assert!(
        delta.content.contains("pg_partitioned_table p")
            && delta
                .content
                .contains("ARRAY['\"external_session_id\"', '\"started_at\"']::TEXT[]"),
        "delta AddUnique must include the desired partition column and append live partition keys:\n{}",
        delta.content
    );
}

#[test]
fn manifest_extension_objects_diff_into_delta_sql() {
    let mut old = table_schema("processing", "jobs", 1);
    old.columns.push(pk_col("job_id"));

    let mut new = old.clone();
    new.extensions.push(DbExtension {
        name: "pg_trgm".to_string(),
        schema: "public".to_string(),
        ..DbExtension::default()
    });
    new.materialized_views.push(MaterializedView {
        name: "job_rollups".to_string(),
        schema: "processing".to_string(),
        query: "SELECT count(*) AS total FROM processing.jobs".to_string(),
        ..MaterializedView::default()
    });
    new.triggers.push(DbTrigger {
        name: "jobs_touch_updated_at".to_string(),
        timing: "BEFORE".to_string(),
        event: "UPDATE".to_string(),
        function: "processing.touch_updated_at()".to_string(),
        for_each: "ROW".to_string(),
        ..DbTrigger::default()
    });

    let old_manifest = CatalogManifest::from_schemas(&[old]).unwrap();
    let plan =
        build_migration_plan(Some(&old_manifest), &[new], &MigrationPlanConfig::default()).unwrap();

    assert!(!plan.blocked);
    assert!(
        plan.changes
            .iter()
            .any(|change| change.kind == ChangeKind::CreateExtension)
    );
    assert!(
        plan.changes
            .iter()
            .any(|change| change.kind == ChangeKind::CreateMaterializedView)
    );
    assert!(
        plan.changes
            .iter()
            .any(|change| change.kind == ChangeKind::CreateTrigger)
    );

    let sql = plan
        .sql_artifacts
        .iter()
        .map(|artifact| artifact.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(sql.contains("CREATE EXTENSION IF NOT EXISTS \"pg_trgm\""));
    assert!(sql.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS"));
    assert!(sql.contains("DROP TRIGGER IF EXISTS \"jobs_touch_updated_at\""));
    assert!(sql.contains("CREATE TRIGGER \"jobs_touch_updated_at\""));
}

#[test]
fn rename_hints_suppress_false_drop_operations() {
    let mut old = table_schema("processing", "jobs", 1);
    old.columns.push(pk_col("job_id"));
    old.columns.push(ProtoColumn {
        field_name: "old_status".to_string(),
        column_name: "old_status".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 2,
        ..ProtoColumn::default()
    });

    let mut new = table_schema("processing", "work_items", 1);
    new.previous_table_name = "jobs".to_string();
    new.columns.push(pk_col("job_id"));
    new.columns.push(ProtoColumn {
        field_name: "status".to_string(),
        column_name: "status".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 2,
        previous_column_name: "old_status".to_string(),
        ..ProtoColumn::default()
    });

    let old_manifest = CatalogManifest::from_schemas(&[old]).unwrap();
    let plan =
        build_migration_plan(Some(&old_manifest), &[new], &MigrationPlanConfig::default()).unwrap();

    assert!(
        plan.changes
            .iter()
            .any(|change| change.kind == ChangeKind::RenameTable)
    );
    assert!(
        plan.changes
            .iter()
            .any(|change| change.kind == ChangeKind::RenameColumn)
    );
    assert!(
        !plan
            .changes
            .iter()
            .any(|change| change.kind == ChangeKind::DropTable)
    );
    assert!(
        !plan
            .changes
            .iter()
            .any(|change| change.kind == ChangeKind::DropColumn)
    );
}

fn table_schema(schema: &str, table: &str, order: i32) -> ProtoSchema {
    ProtoSchema {
        message_name: table.to_string(),
        schema_name: schema.to_string(),
        table_name: table.to_string(),
        migration_order: order,
        is_table: true,
        ..ProtoSchema::default()
    }
}

fn pk_col(name: &str) -> ProtoColumn {
    ProtoColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        sql_type: "UUID".to_string(),
        not_null: true,
        is_primary: true,
        field_number: 1,
        ..ProtoColumn::default()
    }
}

#[test]
fn rich_proto_ast_preserves_nested_oneof_maps_reserved_and_imports() {
    let source = br#"
syntax = "proto3";
package third.party.catalog.v1;

import "google/protobuf/timestamp.proto";
import public "shared/common.proto";

message PortableRecord {
  reserved 8, 12 to 15, "old_name";

  message Profile {
    string display_name = 1;
  }

  enum Status {
    STATUS_UNSPECIFIED = 0;
    STATUS_ACTIVE = 1;
  }

  string id = 1;
  map<string, string> labels = 2;
  Profile profile = 3;

  oneof contact {
    string email = 4;
    bytes phone_hash = 5;
  }
}
"#;

    let ast = parse_ast_source(source, "rich.proto").unwrap();
    assert_eq!(ast.package, "third.party.catalog.v1");
    assert_eq!(ast.imports.len(), 2);
    assert!(ast.imports[1].public);

    let message = ast
        .definitions
        .iter()
        .find_map(|def| match def {
            ProtoDefinition::Message(message) => Some(message),
            _ => None,
        })
        .expect("message");
    assert!(
        message
            .nested
            .iter()
            .any(|def| matches!(def, ProtoDefinition::Message(nested) if nested.name == "Profile"))
    );
    assert_eq!(message.reserved_names, vec!["old_name".to_string()]);
    assert!(
        message
            .reserved_numbers
            .iter()
            .any(|range| range.start == 12 && range.end == 15)
    );
    assert!(
        message
            .fields
            .iter()
            .any(|field| field.name == "labels" && field.field_type == "map")
    );
    assert!(
        message
            .fields
            .iter()
            .any(|field| field.name == "email" && field.oneof_group == "contact")
    );
}

#[test]
fn rich_proto_shapes_compile_to_safe_manifest_columns() {
    let dir = tempfile_dir();
    let proto_path = dir.join("third/party/catalog/v1/portable.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(
        &proto_path,
        r#"
syntax = "proto3";
package third.party.catalog.v1;

option (udb.annotation_version) = "1";

message PortableRecord {
  option (udb.table) = {
    schema_name: "portable"
    table_name: "portable_records"
    is_table: true
  };

  message Profile {
    string display_name = 1;
  }

  string id = 1 [(udb.column) = { primary_key: true sql_type: "UUID" }];
  bytes payload = 2;
  map<string, string> labels = 3;
  repeated string tags = 4;
  google.protobuf.Timestamp observed_at = 5;
  google.protobuf.Int64Value wrapped_count = 6;
  Profile profile = 7;

  oneof contact {
    string email = 8;
    bytes phone_hash = 9;
  }
}
"#,
    )
    .unwrap();

    let config = ParserConfig::default().with_annotation_mode(AnnotationParserMode::Strict);
    let report = parse_file_report(&proto_path, &config).unwrap();
    assert_eq!(report.diagnostics, Vec::new());
    let manifest = CatalogManifest::from_schemas(&report.schemas).unwrap();
    let table = manifest.table("portable", "portable_records").unwrap();

    let column = |name: &str| {
        table
            .columns
            .iter()
            .find(|column| column.column_name == name)
            .unwrap_or_else(|| panic!("missing column {name}"))
    };
    assert_eq!(column("payload").sql_type, "BYTEA");
    assert_eq!(column("labels").sql_type, "JSONB");
    assert_eq!(column("tags").sql_type, "TEXT[]");
    assert_eq!(column("observed_at").sql_type, "TIMESTAMPTZ");
    assert_eq!(column("wrapped_count").sql_type, "BIGINT");
    assert_eq!(column("profile").sql_type, "JSONB");
    assert_eq!(column("email").oneof_group, "contact");
    assert_eq!(column("phone_hash").oneof_group, "contact");
}

#[test]
fn annotation_modes_warn_for_legacy_aliases_and_missing_versions() {
    let dir = tempfile_dir();
    let proto_path = dir.join("third/party/catalog/v1/legacy.proto");
    fs::create_dir_all(proto_path.parent().unwrap()).unwrap();
    fs::write(
        &proto_path,
        r#"
syntax = "proto3";
package third.party.catalog.v1;

message LegacyRecord {
  option (table) = {
    is_table: true
  };
  string id = 1 [(column) = { primary_key: true }];
}
"#,
    )
    .unwrap();

    let compat = parse_file_report(&proto_path, &ParserConfig::default()).unwrap();
    assert!(compat.diagnostics.is_empty());

    let warn_config = ParserConfig::default().with_annotation_mode(AnnotationParserMode::Warn);
    let warn = parse_file_report(&proto_path, &warn_config).unwrap();
    assert!(
        warn.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "legacy_udb_annotation_alias")
    );
    assert!(
        warn.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "udb_annotation_version_missing")
    );
    assert!(
        warn.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "project_schema_path_convention")
    );
}

#[test]
fn malformed_numeric_annotation_values_warn_and_strict_fail_parse() {
    let src = br#"
syntax = "proto3";
package acme.search.v1;

option (udb.annotation_version) = "1";

message SearchEmbedding {
  option (udb.vector_store) = {
    backend: VECTOR_BACKEND_QDRANT
    collection_name: "search_embeddings"
    dimension: "768x"
  };

  string id = 1;
}
"#;

    let warn_config = ParserConfig::default().with_annotation_mode(AnnotationParserMode::Warn);
    let warn = parse_proto_source(src, "acme/search/v1/search_embedding.proto", &warn_config)
        .expect("warn mode should keep parsing");
    let diagnostic = warn
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "malformed_numeric_annotation_value")
        .expect("malformed dimension diagnostic");
    assert!(diagnostic.message.contains("dimension=\"768x\""));
    assert_eq!(
        warn.schemas
            .first()
            .and_then(|schema| schema.vector_store.as_ref())
            .map(|store| store.dimension),
        Some(0)
    );

    let strict_config = ParserConfig::default().with_annotation_mode(AnnotationParserMode::Strict);
    let err = parse_proto_source(src, "acme/search/v1/search_embedding.proto", &strict_config)
        .expect_err("strict mode should fail malformed numeric annotations");
    let rendered = err.to_string();
    assert!(rendered.contains("malformed_numeric_annotation_value"));
    assert!(rendered.contains("dimension=\"768x\""));
}

fn tempfile_dir() -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "udb_parser_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

// ── Lint report tests ─────────────────────────────────────────────────────────

#[test]
fn lint_passes_for_valid_manifest() {
    let mut schema = table_schema("authn", "users", 1);
    schema.columns.push(pk_col("user_id"));

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = lint_catalog(&manifest);

    assert!(report.passed);
    assert_eq!(report.error_count, 0);
    assert_eq!(report.table_count, 1);
    assert_eq!(report.checksum_sha256, manifest.checksum_sha256);
}

#[test]
fn lint_errors_on_missing_primary_key() {
    let mut schema = table_schema("authn", "users", 1);
    schema.columns.push(ProtoColumn {
        field_name: "name".to_string(),
        column_name: "name".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 1,
        ..ProtoColumn::default()
    });

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = lint_catalog(&manifest);

    assert!(!report.passed);
    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "missing_primary_key" && item.severity == LintSeverity::Error)
    );
}

#[test]
fn lint_warns_on_rls_without_policies() {
    let mut schema = table_schema("processing", "docs", 1);
    schema.enable_rls = true;
    schema.columns.push(pk_col("doc_id"));

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = lint_catalog(&manifest);

    assert!(report.passed); // warnings don't block
    assert!(report.items.iter().any(
        |item| item.kind == "rls_enabled_no_policies" && item.severity == LintSeverity::Warning
    ));
}

#[test]
fn lint_errors_on_partition_strategy_without_column() {
    let mut schema = table_schema("events", "audit_log", 1);
    schema.partition_strategy = "RANGE".to_string();
    // partition_column intentionally left empty
    schema.columns.push(pk_col("log_id"));

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = lint_catalog(&manifest);

    assert!(!report.passed);
    assert!(report.items.iter().any(
        |item| item.kind == "partition_column_missing" && item.severity == LintSeverity::Error
    ));
}

#[test]
fn lint_warns_on_pii_not_masked_in_logs() {
    use udb::ProtoColumnSecurity;

    let mut schema = table_schema("authn", "users", 1);
    schema.columns.push(pk_col("user_id"));
    schema.columns.push(ProtoColumn {
        field_name: "email".to_string(),
        column_name: "email".to_string(),
        sql_type: "TEXT".to_string(),
        field_number: 2,
        security: ProtoColumnSecurity {
            is_pii: true,
            mask_in_logs: false, // not masked — should warn
            ..ProtoColumnSecurity::default()
        },
        ..ProtoColumn::default()
    });

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = lint_catalog(&manifest);

    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "pii_not_masked_in_logs"
                && item.column == "email"
                && item.severity == LintSeverity::Warning)
    );
}

#[test]
fn parser_normalizes_scalar_and_structured_field_security_options() {
    let src = br#"
        syntax = "proto3";
        package acme.authn.v1;
        import "udb/core/common/v1/db.proto";
        import "udb/core/common/v1/security.proto";

        message UserSecret {
          option (udb.core.common.v1.pg_table) = {
            schema_name: "authn"
            table_name: "user_secrets"
          };

          string id = 1 [(udb.core.common.v1.pg_column) = { column_name: "id" sql_type: "UUID" primary_key: true }];
          string email = 2 [(udb.core.common.v1.pg_column) = { column_name: "email" sql_type: "TEXT" }, (udb.core.common.v1.pii) = true, (udb.core.common.v1.log_masked) = true, (udb.core.common.v1.data_purpose) = "login"];
          string password_hash = 3 [(udb.core.common.v1.pg_column) = { column_name: "password_hash" sql_type: "TEXT" }, (udb.core.common.v1.sensitive) = true, (udb.core.common.v1.db_column_security) = { secret_classification: SECRET_CLASSIFICATION_CREDENTIAL output_view: OUTPUT_VIEW_STORAGE_ONLY redaction_strategy: REDACTION_STRATEGY_REDACT hashing_algorithm: "argon2id" }];
        }
    "#;

    let report = parse_proto_source(src, "authn/user_secret.proto", &ParserConfig::default())
        .expect("parse");
    assert!(
        report.diagnostics.is_empty(),
        "security annotations must not produce diagnostics: {:?}",
        report.diagnostics
    );
    let schema = report.schemas.first().expect("schema");
    let email = schema
        .columns
        .iter()
        .find(|column| column.field_name == "email")
        .expect("email column");
    assert!(email.security.is_pii);
    assert!(email.security.mask_in_logs);
    assert_eq!(email.security.data_class, "PERSONAL");

    let password = schema
        .columns
        .iter()
        .find(|column| column.field_name == "password_hash")
        .expect("password column");
    assert!(password.security.mask_in_logs);
    assert!(password.security.is_blind_index);
    assert_eq!(password.security.data_class, "CREDENTIAL");
}

#[test]
fn lint_produces_info_for_not_null_without_default() {
    let mut schema = table_schema("processing", "jobs", 1);
    schema.columns.push(pk_col("job_id"));
    schema.columns.push(ProtoColumn {
        field_name: "status".to_string(),
        column_name: "status".to_string(),
        sql_type: "TEXT".to_string(),
        not_null: true,
        // no default_value, no backfill_sql
        field_number: 2,
        ..ProtoColumn::default()
    });

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = lint_catalog(&manifest);

    assert!(report.passed); // info items don't fail lint
    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "not_null_no_default"
                && item.column == "status"
                && item.severity == LintSeverity::Info)
    );
}

#[test]
fn lint_warns_on_jsonb_column_without_is_json_flag() {
    let mut schema = table_schema("documents", "metadata", 1);
    schema.columns.push(pk_col("meta_id"));
    schema.columns.push(ProtoColumn {
        field_name: "payload".to_string(),
        column_name: "payload".to_string(),
        sql_type: "JSONB".to_string(),
        field_number: 2,
        is_json: false, // deliberately not set
        ..ProtoColumn::default()
    });

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = lint_catalog(&manifest);

    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "json_column_not_flagged" && item.column == "payload")
    );
}

// ── Drift report tests ────────────────────────────────────────────────────────

#[test]
fn drift_report_bootstrap_has_all_info_items() {
    let mut schema = table_schema("authn", "users", 1);
    schema.columns.push(pk_col("user_id"));

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = build_drift_report(None, &manifest);

    assert!(report.has_drift);
    assert_eq!(report.old_checksum, "");
    assert_eq!(report.new_checksum, manifest.checksum_sha256);
    assert!(report.auto_safe_count > 0);
    assert_eq!(report.blocked_count, 0);
    // Bootstrap creates tables (Info/SafeAuto)
    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "create_table" && item.severity == DriftSeverity::Info)
    );
}

#[test]
fn drift_report_no_drift_when_manifests_equal() {
    let mut schema = table_schema("authn", "users", 1);
    schema.columns.push(pk_col("user_id"));

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    let report = build_drift_report(Some(&manifest), &manifest);

    assert!(!report.has_drift);
    assert_eq!(report.total_issues, 0);
    assert_eq!(report.critical, 0);
    assert_eq!(report.auto_safe_count, 0);
}

#[test]
fn drift_report_blocked_drop_is_critical() {
    let mut old_schema = table_schema("processing", "jobs", 1);
    old_schema.columns.push(pk_col("job_id"));
    let old = CatalogManifest::from_schemas(&[old_schema]).unwrap();

    // new manifest has no tables → jobs table is dropped (no allow_drop → blocked)
    let new = CatalogManifest::from_schemas(&[]).unwrap();
    let report = build_drift_report(Some(&old), &new);

    assert!(report.has_drift);
    assert!(report.blocked_count > 0);
    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "drop_table" && item.severity == DriftSeverity::Critical)
    );
}

#[test]
fn drift_report_safe_add_column_is_info() {
    let mut old_schema = table_schema("processing", "jobs", 1);
    old_schema.columns.push(pk_col("job_id"));
    let old = CatalogManifest::from_schemas(&[old_schema]).unwrap();

    let mut new_schema = table_schema("processing", "jobs", 1);
    new_schema.columns.push(pk_col("job_id"));
    new_schema.columns.push(ProtoColumn {
        field_name: "priority".to_string(),
        column_name: "priority".to_string(),
        sql_type: "INTEGER".to_string(),
        default_value: "0".to_string(),
        field_number: 2,
        ..ProtoColumn::default()
    });
    let new = CatalogManifest::from_schemas(&[new_schema]).unwrap();

    let report = build_drift_report(Some(&old), &new);

    assert!(report.has_drift);
    assert_eq!(report.blocked_count, 0);
    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "add_column" && item.severity == DriftSeverity::Info)
    );
}

#[test]
fn drift_report_manifest_validation_errors_are_critical() {
    // Build a manifest where a FK references a table that does not exist
    let mut schema = table_schema("processing", "jobs", 1);
    schema.columns.push(pk_col("job_id"));
    schema.columns.push(ProtoColumn {
        field_name: "user_id".to_string(),
        column_name: "user_id".to_string(),
        sql_type: "UUID".to_string(),
        field_number: 2,
        foreign_key: Some(ProtoForeignKey {
            name: "fk_jobs_ghost".to_string(),
            columns: vec!["user_id".to_string()],
            ref_schema: "authn".to_string(),
            ref_table: "ghost_table".to_string(), // does not exist
            ref_columns: vec!["user_id".to_string()],
            ..ProtoForeignKey::default()
        }),
        ..ProtoColumn::default()
    });

    let manifest = CatalogManifest::from_schemas(&[schema]).unwrap();
    assert!(!manifest.validation_errors.is_empty());

    let report = build_drift_report(None, &manifest);
    assert!(report.critical > 0);
    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "manifest_validation_error"
                && item.severity == DriftSeverity::Critical)
    );
}

#[test]
fn docker_and_ci_surface_declares_udb_runtime_contract() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let dockerfile = std::fs::read_to_string(manifest_dir.join("Dockerfile")).unwrap();
    assert!(dockerfile.contains("EXPOSE 50051 50052"));
    assert!(dockerfile.contains("grpc_health_probe"));
    assert!(dockerfile.contains("CMD [\"serve\", \"/app/proto\", \"\", \"0.0.0.0:50051\"]"));

    let compose =
        std::fs::read_to_string(manifest_dir.join("docker-compose.integration.yml")).unwrap();
    assert!(compose.contains("UDB_PG_DSN"));
    assert!(compose.contains("UDB_KAFKA_BROKERS"));
    assert!(compose.contains("\"50051:50051\""));
    assert!(compose.contains("\"50052:50052\""));
    assert!(compose.contains("profiles: [\"broker\"]"));
}

#[test]
#[ignore = "requires Docker; run with UDB_DOCKER_INTEGRATION=1 cargo test -- --ignored docker_compose_integration_stack_smoke"]
fn docker_compose_integration_stack_smoke() {
    if std::env::var("UDB_DOCKER_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("set UDB_DOCKER_INTEGRATION=1 to run Docker-backed UDB smoke tests");
        return;
    }

    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let compose = manifest_dir.join("docker-compose.integration.yml");
    let up = std::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose.to_str().unwrap(),
            "up",
            "-d",
            "--wait",
        ])
        .status()
        .expect("docker compose must be installed");
    assert!(up.success(), "docker compose up failed");

    let ports = [55432, 56379, 56333, 59000, 59192];
    for port in ports {
        let addr = format!("127.0.0.1:{port}");
        let connected = (0..30).any(|_| {
            if std::net::TcpStream::connect(&addr).is_ok() {
                true
            } else {
                std::thread::sleep(std::time::Duration::from_secs(1));
                false
            }
        });
        assert!(connected, "service port {addr} did not become reachable");
    }

    let down = std::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose.to_str().unwrap(),
            "down",
            "-v",
            "--remove-orphans",
        ])
        .status()
        .expect("docker compose down must run");
    assert!(down.success(), "docker compose down failed");
}

#[test]
#[ignore = "requires Docker; run with UDB_DOCKER_INTEGRATION=1 cargo test -- --ignored live_backend_drift_repair_smoke"]
fn live_backend_drift_repair_smoke() {
    if std::env::var("UDB_DOCKER_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("set UDB_DOCKER_INTEGRATION=1 to run Docker-backed UDB drift/repair tests");
        return;
    }

    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let compose = manifest_dir.join("docker-compose.integration.yml");
    let compose_file = compose.to_str().unwrap();
    let up = std::process::Command::new("docker")
        .args(["compose", "-f", compose_file, "up", "-d", "--wait"])
        .status()
        .expect("docker compose must be installed");
    assert!(up.success(), "docker compose up failed");

    let create_drifted_table = "CREATE SCHEMA IF NOT EXISTS drift; \
        DROP TABLE IF EXISTS drift.jobs; \
        CREATE TABLE drift.jobs (job_id UUID PRIMARY KEY);";
    let create_status = std::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file,
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "udb",
            "-d",
            "udb",
            "-c",
            create_drifted_table,
        ])
        .status()
        .expect("psql create drifted table must run");
    assert!(create_status.success(), "failed to create drifted table");

    let repair = plan_repairs(&[LintInput {
        lint_kind: "missing_column".to_string(),
        schema: "drift".to_string(),
        table: "jobs".to_string(),
        column: "status".to_string(),
        sql_type: "TEXT".to_string(),
        default_value: String::new(),
    }]);
    let ddl = repair.auto_safe_decisions().next().unwrap().ddl.clone();
    let repair_status = std::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file,
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "udb",
            "-d",
            "udb",
            "-c",
            &ddl,
        ])
        .status()
        .expect("psql repair must run");
    assert!(repair_status.success(), "failed to apply repair DDL");

    let verify = std::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file,
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "udb",
            "-d",
            "udb",
            "-tAc",
            "SELECT column_name FROM information_schema.columns WHERE table_schema='drift' AND table_name='jobs' AND column_name='status';",
        ])
        .output()
        .expect("psql verify must run");
    assert!(verify.status.success(), "repair verification query failed");
    let stdout = String::from_utf8_lossy(&verify.stdout);
    assert!(stdout.contains("status"), "status column was not repaired");

    let down = std::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file,
            "down",
            "-v",
            "--remove-orphans",
        ])
        .status()
        .expect("docker compose down must run");
    assert!(down.success(), "docker compose down failed");
}

#[test]
fn drift_report_hint_warning_is_warning_severity() {
    let mut old_schema = table_schema("processing", "jobs", 1);
    old_schema.columns.push(pk_col("job_id"));
    let old = CatalogManifest::from_schemas(&[old_schema]).unwrap();

    // New table with a stale previous_table_name that doesn't match old manifest
    let mut new_schema = table_schema("processing", "work_items", 1);
    new_schema.previous_table_name = "nonexistent_old_table".to_string();
    new_schema.columns.push(pk_col("job_id"));
    let new = CatalogManifest::from_schemas(&[new_schema]).unwrap();

    let report = build_drift_report(Some(&old), &new);

    assert!(report.hint_warnings > 0);
    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == "hint_warning" && item.severity == DriftSeverity::Warning)
    );
}
