//! Cross-backend acceptance tests (U2 step 5).
//!
//! Proves the upgrade-plan's acceptance gate: **the same `LogicalRead`/
//! `LogicalWrite`/`LogicalDelete` compiles to semantically-equivalent
//! shapes on both Postgres SQL and MongoDB JSON**. If a future change
//! makes the IR carry backend-specific semantics by accident, these tests
//! catch it before the multi-leg planner (step 6) inherits the drift.

#![cfg(test)]

use crate::backend::BackendKind;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::ir::compile::mongodb::MongoDbCompiler;
use crate::ir::compile::postgres::PostgresCompiler;
use crate::ir::compile::{CompileContext, CompiledRendering, Compiler};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    ConflictStrategy, LogicalDelete, LogicalRead, LogicalRecord, LogicalWrite,
};
use crate::ir::projection::{LogicalPagination, LogicalProjection, LogicalSort, SortDirection};
use crate::ir::value::LogicalValue;

fn shared_manifest() -> CatalogManifest {
    let table = ManifestTable {
        message_name: "acme.billing.v1.Customer".to_string(),
        schema: "public".to_string(),
        table: "customers".to_string(),
        primary_key: vec!["id".to_string()],
        columns: vec![
            ManifestColumn {
                field_name: "id".into(),
                column_name: "id".into(),
                proto_type: "string".into(),
                sql_type: "uuid".into(),
                is_primary: true,
                not_null: true,
                ..Default::default()
            },
            ManifestColumn {
                field_name: "name".into(),
                column_name: "name".into(),
                proto_type: "string".into(),
                sql_type: "text".into(),
                ..Default::default()
            },
            ManifestColumn {
                field_name: "email".into(),
                column_name: "email".into(),
                proto_type: "string".into(),
                sql_type: "text".into(),
                ..Default::default()
            },
            ManifestColumn {
                field_name: "tenant_id".into(),
                column_name: "tenant_id".into(),
                proto_type: "string".into(),
                sql_type: "text".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    CatalogManifest {
        tables: vec![table],
        ..Default::default()
    }
}

/// One `LogicalRead` lowered to both backends. Asserts each compiler picks
/// the right shape, references the right field-name convention, and bind-
/// parameterises identically (no value interpolation on either side).
#[test]
fn same_logical_read_compiles_to_postgres_and_mongo() {
    let m = shared_manifest();
    let ctx = CompileContext::new(&m).with_tenant("acme-1");

    let read = LogicalRead::message("acme.billing.v1.Customer")
        .with_filter(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("acme-1".into()),
            },
            LogicalFilter::Comparison {
                field: "email".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("alice@acme.com".into()),
            },
        ]))
        .with_projection(LogicalProjection::fields(["id".into(), "name".into()]))
        .with_sort(vec![LogicalSort {
            field: "name".into(),
            direction: SortDirection::Asc,
            nulls: crate::ir::projection::NullOrder::Default,
        }])
        .with_pagination(LogicalPagination::limit(50));

    // ── Postgres ─────────────────────────────────────────────────────────
    let pg = PostgresCompiler
        .compile_read(&read, &ctx)
        .expect("pg compile");
    match pg {
        CompiledRendering::Sql {
            backend,
            statement,
            params,
        } => {
            assert_eq!(backend, BackendKind::Postgres);
            assert!(
                statement.starts_with("SELECT \"id\", \"name\" FROM \"public\".\"customers\""),
                "unexpected SELECT: {statement}"
            );
            assert!(statement.contains("WHERE"));
            assert!(statement.contains("\"tenant_id\" = $1"));
            assert!(statement.contains("\"email\" = $2"));
            assert!(statement.contains("ORDER BY \"name\" ASC"));
            assert!(statement.ends_with("LIMIT 50"));
            assert_eq!(
                params,
                vec![
                    // filter tenant_id, filter email, then F6's defense-in-depth
                    // tenant backstop (context_predicates re-asserts the claim
                    // tenant on the read even though the caller filter already
                    // scoped it — redundant here, load-bearing when a caller omits
                    // the tenant predicate). All bound positionally, no interpolation.
                    LogicalValue::String("acme-1".into()),
                    LogicalValue::String("alice@acme.com".into()),
                    LogicalValue::String("acme-1".into()),
                ],
                "params must be bound positionally with no interpolation"
            );
        }
        other => panic!("Postgres compiler emitted non-Sql rendering: {other:?}"),
    }

    // ── MongoDB ─────────────────────────────────────────────────────────
    let mongo = MongoDbCompiler
        .compile_read(&read, &ctx)
        .expect("mongo compile");
    match mongo {
        CompiledRendering::Json {
            backend,
            path,
            body,
            ..
        } => {
            assert_eq!(backend, BackendKind::Mongodb);
            assert_eq!(path, "/action/find");
            assert_eq!(body["collection"], "customers");
            // C7/C8: the Mongo compiler now wraps the user filter in
            // a `$and` together with the active context's tenant
            // predicate. The manifest declares a real `tenant_id`
            // column, so the compiler injects under that resolved
            // column name (the synthetic `_tenant_id` fallback is only
            // used when no tenant column is declared). Outer shape:
            //   { $and: [ <user_filter>, { tenant_id: "acme-1" } ] }
            // The user filter is itself an `$and` over the original
            // two equality predicates the test built.
            let outer_and = body["filter"]["$and"].as_array().expect("outer $and");
            assert_eq!(outer_and.len(), 2);
            // [0] is the user filter (itself an $and).
            let inner_and = outer_and[0]["$and"].as_array().expect("inner $and");
            assert_eq!(inner_and.len(), 2);
            assert_eq!(inner_and[0]["tenant_id"]["$eq"], "acme-1");
            assert_eq!(inner_and[1]["email"]["$eq"], "alice@acme.com");
            // [1] is the context-injected tenant predicate.
            assert_eq!(outer_and[1]["tenant_id"], "acme-1");
            // Projection sets both fields to 1.
            assert_eq!(body["projection"]["id"], 1);
            assert_eq!(body["projection"]["name"], 1);
            // Sort = { name: 1 }.
            assert_eq!(body["sort"]["name"], 1);
            assert_eq!(body["limit"], 50);
        }
        other => panic!("Mongo compiler emitted non-Json rendering: {other:?}"),
    }
}

/// One `LogicalWrite` lowered to both backends. Postgres emits ON CONFLICT
/// DO UPDATE; Mongo emits updateOne with upsert=true. The IR carries the
/// **same `ConflictStrategy::Update { fields: ... }`** in both cases.
#[test]
fn same_logical_write_upsert_compiles_to_both_backends() {
    let m = shared_manifest();
    let ctx = CompileContext::new(&m);

    let mut record = LogicalRecord::new();
    record.insert("id".into(), LogicalValue::String("cust-1".into()));
    record.insert("name".into(), LogicalValue::String("Alice".into()));
    record.insert(
        "email".into(),
        LogicalValue::String("alice@acme.com".into()),
    );

    let write = LogicalWrite {
        message_type: "acme.billing.v1.Customer".into(),
        records: vec![record],
        conflict: ConflictStrategy::update(vec!["name".into(), "email".into()]),
        return_fields: vec![],
    };

    let pg = PostgresCompiler.compile_write(&write, &ctx).expect("pg");
    if let CompiledRendering::Sql {
        statement, params, ..
    } = pg
    {
        assert!(statement.starts_with("INSERT INTO \"public\".\"customers\""));
        assert!(statement.contains("ON CONFLICT (\"id\")"));
        assert!(statement.contains("\"name\" = EXCLUDED.\"name\""));
        assert!(statement.contains("\"email\" = EXCLUDED.\"email\""));
        // Three bind params: id/name/email values (column order from
        // BTreeMap is deterministic so the SQL is stable too).
        assert_eq!(params.len(), 3);
    } else {
        panic!("expected Sql rendering from PG write");
    }

    let mongo = MongoDbCompiler.compile_write(&write, &ctx).expect("mongo");
    if let CompiledRendering::Json { path, body, .. } = mongo {
        assert_eq!(path, "/action/updateOne");
        assert_eq!(body["filter"]["id"], "cust-1");
        assert_eq!(body["update"]["$set"]["name"], "Alice");
        assert_eq!(body["update"]["$set"]["email"], "alice@acme.com");
        assert_eq!(body["upsert"], true);
    } else {
        panic!("expected Json rendering from Mongo write");
    }
}

/// `LogicalDelete` with a real filter compiles on both; with no filter
/// compiles on neither. Proves the safety contract holds uniformly.
#[test]
fn delete_safety_contract_is_uniform() {
    let m = shared_manifest();
    let ctx = CompileContext::new(&m);

    let safe = LogicalDelete {
        message_type: "acme.billing.v1.Customer".into(),
        filter: LogicalFilter::Comparison {
            field: "id".into(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("cust-1".into()),
        },
        return_fields: vec![],
    };
    assert!(PostgresCompiler.compile_delete(&safe, &ctx).is_ok());
    assert!(MongoDbCompiler.compile_delete(&safe, &ctx).is_ok());

    let unbounded = LogicalDelete {
        message_type: "acme.billing.v1.Customer".into(),
        filter: LogicalFilter::always(),
        return_fields: vec![],
    };
    assert!(
        PostgresCompiler.compile_delete(&unbounded, &ctx).is_err(),
        "PG must refuse unbounded delete"
    );
    assert!(
        MongoDbCompiler.compile_delete(&unbounded, &ctx).is_err(),
        "Mongo must refuse unbounded delete"
    );
}

/// Cross-backend compiler acceptance: the same logical select runs against
/// Postgres, MongoDB, Neo4j, and ClickHouse compilers. Asserts each emits its
/// native wire shape and references the manifest's column/field correctly.
#[test]
fn same_logical_select_compiles_to_four_backends() {
    use crate::ir::compile::clickhouse::ClickHouseCompiler;
    use crate::ir::compile::neo4j::Neo4jCompiler;

    let m = shared_manifest();
    let ctx = CompileContext::new(&m);

    let read =
        LogicalRead::message("acme.billing.v1.Customer").with_filter(LogicalFilter::Comparison {
            field: "email".into(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("alice@acme.com".into()),
        });

    // Postgres → SQL with $1.
    match PostgresCompiler.compile_read(&read, &ctx).expect("pg") {
        CompiledRendering::Sql {
            statement, params, ..
        } => {
            assert!(statement.contains("\"email\" = $1"));
            assert_eq!(params.len(), 1);
        }
        other => panic!("PG: {other:?}"),
    }

    // MongoDB → /action/find JSON.
    match MongoDbCompiler.compile_read(&read, &ctx).expect("mongo") {
        CompiledRendering::Json { path, body, .. } => {
            assert_eq!(path, "/action/find");
            assert_eq!(body["filter"]["email"]["$eq"], "alice@acme.com");
        }
        other => panic!("Mongo: {other:?}"),
    }

    // Neo4j → Cypher MATCH … RETURN n.
    match Neo4jCompiler.compile_read(&read, &ctx).expect("neo4j") {
        CompiledRendering::Json { body, .. } => {
            let stmt = body["statements"][0]["statement"].as_str().unwrap();
            assert!(stmt.starts_with("MATCH (n:`customers`)"));
            assert!(stmt.contains("WHERE n.`email` = $p0"));
            assert_eq!(body["statements"][0]["parameters"]["p0"], "alice@acme.com");
        }
        other => panic!("Neo4j: {other:?}"),
    }

    // ClickHouse → SQL with ? placeholders + backticks.
    match ClickHouseCompiler.compile_read(&read, &ctx).expect("ch") {
        CompiledRendering::Sql {
            statement, params, ..
        } => {
            assert!(statement.starts_with("SELECT * FROM `public`.`customers`"));
            assert!(statement.contains("WHERE `email` = ?"));
            assert_eq!(params, vec![LogicalValue::String("alice@acme.com".into())]);
        }
        other => panic!("CH: {other:?}"),
    }
}

/// Cache key is identical across two compilations of the same operation
/// (proves the canonical key is stable) and changes when the manifest
/// checksum changes (proves schema migrations invalidate the cache).
#[test]
fn canonical_cache_key_is_stable_and_schema_versioned() {
    use crate::ir::cache_key::{CacheKeyContext, canonical_cache_key};

    let m = shared_manifest();
    let ctx = CompileContext::new(&m).with_tenant("acme-1");
    let read =
        LogicalRead::message("acme.billing.v1.Customer").with_filter(LogicalFilter::Comparison {
            field: "email".into(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("alice@acme.com".into()),
        });
    let r1 = PostgresCompiler.compile_read(&read, &ctx).unwrap();
    let r2 = PostgresCompiler.compile_read(&read, &ctx).unwrap();

    let key_ctx_v1 = CacheKeyContext {
        backend: &BackendKind::Postgres,
        instance: Some("primary"),
        tenant_id: Some("acme-1"),
        project_id: Some("billing"),
        manifest_checksum: "checksum-v1",
    };
    let key_ctx_v2 = CacheKeyContext {
        manifest_checksum: "checksum-v2",
        ..key_ctx_v1
    };

    let k1 = canonical_cache_key(&r1, &key_ctx_v1);
    let k1_again = canonical_cache_key(&r2, &key_ctx_v1);
    let k2 = canonical_cache_key(&r1, &key_ctx_v2);

    assert_eq!(k1, k1_again, "identical inputs must produce identical keys");
    assert_ne!(k1, k2, "schema migration must invalidate the cached entry");
}

/// Multi-leg plan: a single `LogicalRead` lowered to two backends and
/// executed in parallel via `execute_plan`. Demonstrates the U2 acceptance
/// gate "logical read can run Postgres plus Qdrant/Mongo/ClickHouse legs
/// in parallel and merge results under a declared partial-failure policy."
#[tokio::test]
async fn fanout_plan_executes_pg_and_mongo_concurrently_with_partial_failure_policy() {
    use crate::ir::plan::{ExecutionPlan, Leg, PartialFailurePolicy, execute_plan};

    let m = shared_manifest();
    let ctx = CompileContext::new(&m);
    let read =
        LogicalRead::message("acme.billing.v1.Customer").with_filter(LogicalFilter::Comparison {
            field: "id".into(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("cust-1".into()),
        });

    let pg = PostgresCompiler.compile_read(&read, &ctx).unwrap();
    let mongo = MongoDbCompiler.compile_read(&read, &ctx).unwrap();

    let plan = ExecutionPlan::fanout(vec![
        Leg::new("customer_row", pg).with_instance("primary"),
        Leg::new("customer_doc", mongo),
    ])
    .with_policy(PartialFailurePolicy::Quorum { required: 1 });

    // Stub executor: PG leg succeeds, Mongo leg "fails" — quorum=1 still
    // satisfies the plan.
    let verdict = execute_plan(&plan, |leg| async move {
        match leg.label.as_str() {
            "customer_row" => Ok::<_, String>(serde_json::json!({"backend":"pg","ok":true})),
            "customer_doc" => Err("mongo unreachable".into()),
            other => panic!("unexpected leg label: {other}"),
        }
    })
    .await;

    assert!(
        verdict.satisfied,
        "quorum=1 with one ok leg must satisfy the plan"
    );
    assert_eq!(verdict.legs.len(), 2);
    // Order must match declaration regardless of completion order.
    assert_eq!(verdict.legs[0].label, "customer_row");
    assert_eq!(verdict.legs[1].label, "customer_doc");
    assert!(verdict.legs[0].outcome.is_ok());
    assert!(verdict.legs[1].outcome.is_err());
}

/// Raw-dispatch policy: forbids by default, audit-records on opt-in.
#[test]
fn raw_dispatch_policy_default_forbids() {
    use crate::ir::raw_dispatch::{RawDispatchAudit, RawDispatchPolicy};

    assert_eq!(RawDispatchPolicy::default(), RawDispatchPolicy::Forbid);
    assert!(!RawDispatchPolicy::default().permits());

    // AllowWithAudit emits records that include enough context for
    // operator deprecation tracking.
    let audit = RawDispatchAudit::record(
        Some("acme-1"),
        Some("billing"),
        "mongodb",
        Some("default"),
        "acme.billing.v1.Customer",
        r#"{"collection":"customers","filter":{"email":"alice@acme.com"}}"#,
        RawDispatchPolicy::AllowWithAudit,
    );
    assert_eq!(audit.tenant_id.as_deref(), Some("acme-1"));
    assert_eq!(audit.backend, "mongodb");
    assert!(audit.body_preview.contains("customers"));
}

/// Capability mismatches surface typed errors on the right backend, not
/// silent fallbacks. Proves the IR carries enough info to reject at
/// compile time rather than mid-execution.
#[test]
fn capability_mismatches_are_compile_errors() {
    // Search IS now supported by Postgres (pgvector) and Mongo
    // ($vectorSearch). But the shared_manifest() fixture has no
    // pgvector column, so the Postgres path surfaces a typed
    // Malformed error explaining the missing column. MongoDB doesn't
    // require a typed vector column (Atlas Search drives off an
    // operator-configured index), so the compile succeeds.
    //
    // The right portability assertion is no longer "both reject" but
    // "the compiler that can't lower it returns a typed error rather
    // than emitting invalid output." Redis still has no vector path
    // and is a clean stand-in for that contract.
    let m = shared_manifest();
    let ctx = CompileContext::new(&m);

    let search = crate::ir::operations::LogicalSearch {
        message_type: "acme.billing.v1.Customer".into(),
        vector: Some(vec![0.0; 4]),
        text_query: None,
        filter: None,
        top_k: 5,
        score_threshold: None,
        require_hybrid: false,
        with_vector: false,
        with_payload: true,
    };
    let pg = PostgresCompiler.compile_search(&search, &ctx).unwrap_err();
    let redis = crate::ir::compile::redis::RedisCompiler
        .compile_search(&search, &ctx)
        .unwrap_err();
    // Postgres rejects with Malformed (missing pgvector column).
    assert_eq!(pg.code(), "malformed");
    // Redis rejects with OperationNotSupported (no search surface).
    assert_eq!(redis.code(), "operation_not_supported");
    // Mongo lowers cleanly — Atlas $vectorSearch doesn't need a
    // manifest column declaration.
    let mongo_ok = MongoDbCompiler.compile_search(&search, &ctx);
    assert!(mongo_ok.is_ok());
}
