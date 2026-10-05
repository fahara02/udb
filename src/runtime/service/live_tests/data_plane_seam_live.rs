//! LIVE served seam tests for the 0.5.26 relational data plane + authorization
//! fixes (Phase 2 of the e2e completion plan).
//!
//! Every test drives the REAL DataBroker handlers (directly, or through a
//! loopback tonic server for the client-streaming RPCs) against a live
//! Postgres and asserts BOTH the user-visible outcome AND a raw, privileged
//! read-back of what physically happened. The authorization tests run on
//! [`dp_service_deny`] — `abac_default_allow` OFF, a deny-by-default snapshot
//! PG-warmed from `udb_authz.policy_rules` — so a served deny is proven against
//! the production posture, not the default-allow test harness.
//!
//! All tests are `#[ignore]`d (the CI live lane runs every ignored lib test,
//! serially); inside that lane a missing DSN fails rather than skips.

use futures::stream;
use serde_json::json;
use tonic::{Code, Request};
use uuid::Uuid;

use super::data_plane_live::{
    col, create_schema, dp_insert_allow_rule, dp_live_pg_dsn, dp_pool, dp_prepare_deny_db,
    dp_service, dp_service_deny, dp_warm_authz, install_dp_security, raw_tenant_and_status,
    serve_data_broker, served_upsert, teardown, widget_manifest, with_ctx,
};
use crate::generation::{CatalogManifest, ManifestTable, ManifestTableSecurity};
use crate::proto::data_broker_client::DataBrokerClient;
use crate::proto::data_broker_server::DataBroker;
use crate::proto::{Chunk, DeleteRequest, Mutation, SelectRequest, Sort, UpsertRequest, tx_status};
use crate::runtime::catalog::DEFAULT_PROJECT_ID;
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::ensure_system_catalog;

// ── shared helpers ───────────────────────────────────────────────────────────

/// `with_ctx` plus an explicit project and a named service identity — the
/// subject the deny-profile allow rules are bound to.
fn as_caller<T>(message: T, tenant: &str, project: &str, subject: &str) -> Request<T> {
    let mut request = with_ctx(message, tenant);
    let md = request.metadata_mut();
    md.insert("x-udb-project-id", project.parse().unwrap());
    md.insert("x-service-identity", subject.parse().unwrap());
    request
}

/// Make `project` a second ACTIVE project serving the broker's own manifest,
/// so a request in it reaches the policy / data path instead of the catalog
/// gate.
async fn activate_second_project(svc: &DataBrokerService, project: &str) {
    let checksum = svc
        .catalog
        .stage_catalog(
            svc.manifest.clone(),
            project.to_string(),
            "dp-seam-second-project".to_string(),
            "exact".to_string(),
        )
        .await
        .unwrap_or_else(|err| panic!("stage catalog for {project}: {err}"));
    svc.catalog
        .activate_catalog_for(project, &checksum)
        .await
        .unwrap_or_else(|err| panic!("activate catalog for {project}: {err}"));
}

/// Raw privileged row count by id (bypasses every served scope).
async fn raw_count(pool: &sqlx::PgPool, schema: &str, table: &str, id: &str) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema}\".\"{table}\" WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("raw privileged row count")
}

/// Drain a server-streaming response, returning the first error status (from
/// the call itself or any stream item) — `None` when the stream ended cleanly.
async fn first_stream_error<T>(
    result: Result<tonic::Response<tonic::Streaming<T>>, tonic::Status>,
) -> Option<tonic::Status> {
    let mut stream = match result {
        Ok(response) => response.into_inner(),
        Err(status) => return Some(status),
    };
    loop {
        match stream.message().await {
            Ok(Some(_)) => continue,
            Ok(None) => return None,
            Err(status) => return Some(status),
        }
    }
}

/// Drive one `BeginTx`, tolerating a refusal raised before the response
/// stream opens (authorization / validation of the drained mutations) as well
/// as one surfaced mid-stream. Returns (committed, first error).
async fn begin_tx_outcome(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    request: Request<stream::Iter<std::vec::IntoIter<Mutation>>>,
) -> (bool, Option<tonic::Status>) {
    let mut stream = match client.begin_tx(request).await {
        Ok(response) => response.into_inner(),
        Err(status) => return (false, Some(status)),
    };
    let mut committed = false;
    loop {
        match stream.message().await {
            Ok(Some(status)) => {
                if status.state == tx_status::State::TxStateCommitted as i32 {
                    committed = true;
                }
            }
            Ok(None) => return (committed, None),
            Err(status) => return (committed, Some(status)),
        }
    }
}

fn upsert_mutation(message: &str, record: serde_json::Value) -> Mutation {
    Mutation {
        message_type: message.to_string(),
        operation: "upsert".to_string(),
        record_json: serde_json::to_vec(&record).unwrap(),
        commit: true,
        tx_id: Uuid::new_v4().to_string(),
        ..Mutation::default()
    }
}

/// One tenant-scoped table manifest with the given columns / primary key.
fn table_manifest(
    schema: &str,
    table: &str,
    message: &str,
    columns: Vec<crate::generation::ManifestColumn>,
    primary_key: &[&str],
    project_column: &str,
) -> CatalogManifest {
    CatalogManifest {
        tables: vec![ManifestTable {
            proto_package: "acme.dp.v1".to_string(),
            message_name: message.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            primary_key: primary_key.iter().map(|key| key.to_string()).collect(),
            table_security: ManifestTableSecurity {
                tenant_column: "tenant_id".to_string(),
                project_column: project_column.to_string(),
                ..ManifestTableSecurity::default()
            },
            columns,
            ..ManifestTable::default()
        }],
        ..CatalogManifest::default()
    }
}

/// Status + message of a refusal, for "indistinguishable outcome" comparisons.
fn outcome(result: Result<impl Sized, tonic::Status>) -> (Code, String) {
    match result {
        Ok(_) => (Code::Ok, String::new()),
        Err(status) => (status.code(), status.message().to_string()),
    }
}

// ── A3: compare-and-swap is not an existence / value oracle ─────────────────

/// A3 — a CAS write naming another tenant's row reads EXACTLY like a CAS
/// against an absent row (same code, same message), on a plain-PK table and on
/// a tenant-in-PK table, and the foreign row is untouched.
///
/// Revert-proof: restore the `!key_columns.contains(tenant column)` skip in
/// `scoped_locked_row_lookup` and the tenant-in-PK delete locates the victim's
/// row, its `expected` matches, and the outcome differs from the absent-row
/// FailedPrecondition — the equality assertion fails.
#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn served_cas_on_a_foreign_row_reads_like_an_absent_row_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let attacker = Uuid::new_v4().to_string();
    let victim = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".scoped \
         (id TEXT NOT NULL, tenant_id TEXT NOT NULL, status TEXT, PRIMARY KEY (id, tenant_id))"
    ))
    .execute(&pool)
    .await
    .expect("create tenant-in-PK table");

    // Plain-PK table: single-row CAS upsert.
    const WIDGET: &str = "acme.dp.v1.Widget";
    let svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;
    let victim_id = format!("victim-{}", Uuid::new_v4().simple());
    served_upsert(
        &svc,
        &victim,
        WIDGET,
        json!({"id": victim_id, "tenant_id": victim, "status": "ORIGINAL"}),
        "",
    )
    .await
    .expect("seed victim widget");
    let cas_upsert = |id: String| UpsertRequest {
        message_type: WIDGET.to_string(),
        record_json: serde_json::to_vec(
            &json!({"id": id, "tenant_id": attacker, "status": "PWNED"}),
        )
        .unwrap(),
        expected: json_to_struct(&json!({"status": "ORIGINAL"})),
        ..UpsertRequest::default()
    };
    let foreign = outcome(
        svc.upsert(with_ctx(cas_upsert(victim_id.clone()), &attacker))
            .await,
    );
    let absent = outcome(
        svc.upsert(with_ctx(
            cas_upsert(format!("absent-{}", Uuid::new_v4().simple())),
            &attacker,
        ))
        .await,
    );
    assert_ne!(
        foreign.0,
        Code::Ok,
        "a CAS on a foreign row must not succeed"
    );
    assert_eq!(
        foreign, absent,
        "a CAS on a foreign row must be indistinguishable from one on an absent row"
    );
    assert_eq!(
        raw_tenant_and_status(&pool, &schema, "widgets", &victim_id).await,
        Some((victim.clone(), Some("ORIGINAL".to_string()))),
        "the foreign widget must be untouched"
    );

    // Tenant-in-PK table: the caller's KEY names the victim tenant.
    const SCOPED: &str = "acme.dp.v1.Scoped";
    let mut tenant_col = col("tenant_id", "TEXT", true);
    tenant_col.is_tenant_column = true;
    let scoped_svc = dp_service(
        &dsn,
        table_manifest(
            &schema,
            "scoped",
            "Scoped",
            vec![
                col("id", "TEXT", true),
                tenant_col,
                col("status", "TEXT", false),
            ],
            &["id", "tenant_id"],
            "",
        ),
    )
    .await;
    let scoped_id = format!("scoped-{}", Uuid::new_v4().simple());
    served_upsert(
        &scoped_svc,
        &victim,
        SCOPED,
        json!({"id": scoped_id, "tenant_id": victim, "status": "ORIGINAL"}),
        "",
    )
    .await
    .expect("seed victim tenant-in-PK row");
    let cas_delete = |id: String| DeleteRequest {
        message_type: SCOPED.to_string(),
        filter: json_to_struct(&json!({"id": id, "tenant_id": victim})),
        expected: json_to_struct(&json!({"status": "ORIGINAL"})),
        ..DeleteRequest::default()
    };
    let foreign = outcome(
        scoped_svc
            .delete(with_ctx(cas_delete(scoped_id.clone()), &attacker))
            .await,
    );
    let absent = outcome(
        scoped_svc
            .delete(with_ctx(
                cas_delete(format!("absent-{}", Uuid::new_v4().simple())),
                &attacker,
            ))
            .await,
    );
    assert_ne!(
        foreign.0,
        Code::Ok,
        "a foreign tenant-in-PK CAS must not succeed"
    );
    assert_eq!(
        foreign, absent,
        "a tenant-in-PK CAS naming the victim tenant must read exactly like an absent row"
    );
    assert_eq!(
        raw_tenant_and_status(&pool, &schema, "scoped", &scoped_id).await,
        Some((victim.clone(), Some("ORIGINAL".to_string()))),
        "the victim's tenant-in-PK row must be untouched"
    );

    teardown(&pool, &schema, &attacker).await;
    teardown(&pool, &schema, &victim).await;
}

// ── B1: the wildcard message type is refused on every data entry point ─────

/// B1 — `"*"` and `""` are refused with InvalidArgument on served Select,
/// Upsert and every BatchUpsert / BatchSelect item, and nothing is written.
///
/// Revert-proof: drop the per-item `reject_wildcard_data_message_type` calls in
/// the batch handlers and the BatchUpsert item reaches the policy gate instead
/// (default-allow here), so no InvalidArgument is returned.
#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn served_wildcard_message_type_is_refused_on_every_data_entry_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    let manifest = widget_manifest(&schema, "widgets", "Widget", false);
    let svc = dp_service(&dsn, manifest.clone()).await;
    let server_svc = dp_service(&dsn, manifest).await;
    let (mut client, shutdown, handle) = serve_data_broker(server_svc).await;

    for wildcard in ["*", ""] {
        let id = format!("wild-{}", Uuid::new_v4().simple());
        let record = json!({"id": id, "tenant_id": tenant, "status": "X"});
        let err = svc
            .select(with_ctx(
                SelectRequest {
                    message_type: wildcard.to_string(),
                    ..SelectRequest::default()
                },
                &tenant,
            ))
            .await
            .expect_err("a wildcard Select must be refused");
        assert_eq!(err.code(), Code::InvalidArgument, "Select {wildcard:?}");

        let err = served_upsert(&svc, &tenant, wildcard, record.clone(), "")
            .await
            .expect_err("a wildcard Upsert must be refused");
        assert_eq!(err.code(), Code::InvalidArgument, "Upsert {wildcard:?}");

        let batch = vec![UpsertRequest {
            message_type: wildcard.to_string(),
            record_json: serde_json::to_vec(&record).unwrap(),
            ..UpsertRequest::default()
        }];
        let err = first_stream_error(
            client
                .batch_upsert(with_ctx(stream::iter(batch), &tenant))
                .await,
        )
        .await
        .expect("a wildcard BatchUpsert item must be refused");
        assert_eq!(
            err.code(),
            Code::InvalidArgument,
            "BatchUpsert {wildcard:?}"
        );

        let batch = vec![SelectRequest {
            message_type: wildcard.to_string(),
            ..SelectRequest::default()
        }];
        let err = first_stream_error(
            client
                .batch_select(with_ctx(stream::iter(batch), &tenant))
                .await,
        )
        .await
        .expect("a wildcard BatchSelect item must be refused");
        assert_eq!(
            err.code(),
            Code::InvalidArgument,
            "BatchSelect {wildcard:?}"
        );

        assert_eq!(
            raw_count(&pool, &schema, "widgets", &id).await,
            0,
            "a refused wildcard write must leave nothing behind"
        );
    }

    let _ = shutdown.send(());
    let _ = handle.await;
    teardown(&pool, &schema, &tenant).await;
}

// ── A1 / A5 / A9 / A4 on the default-allow served harness ───────────────────

/// A1 (project column) — a caller in project P2 upserting a key that belongs to
/// a row in project P1 of the SAME tenant is refused, and the row keeps its
/// project and values.
///
/// Revert-proof: drop the project guard from the upsert conflict branch and the
/// P2 upsert re-homes the row (raw read shows project P2 / status PWNED).
#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn served_upsert_cannot_take_over_another_projects_row_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let other_project = format!("a1-other-{}", Uuid::new_v4().simple());
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".pwidgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, project_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create project-scoped widgets");
    const MSG: &str = "acme.dp.v1.PWidget";
    let mut project_col = col("project_id", "TEXT", false);
    project_col.is_project_column = true;
    project_col.not_null = true;
    let svc = dp_service(
        &dsn,
        table_manifest(
            &schema,
            "pwidgets",
            "PWidget",
            vec![
                col("id", "TEXT", true),
                col("tenant_id", "TEXT", false),
                project_col,
                col("status", "TEXT", false),
            ],
            &["id"],
            "project_id",
        ),
    )
    .await;
    activate_second_project(&svc, &other_project).await;

    let id = format!("a1-{}", Uuid::new_v4().simple());
    svc.upsert(as_caller(
        UpsertRequest {
            message_type: MSG.to_string(),
            record_json: serde_json::to_vec(&json!({
                "id": id, "tenant_id": tenant, "project_id": DEFAULT_PROJECT_ID, "status": "ORIGINAL"
            }))
            .unwrap(),
            ..UpsertRequest::default()
        },
        &tenant,
        DEFAULT_PROJECT_ID,
        "svc-a1",
    ))
    .await
    .expect("seed the default-project row");

    let result = svc
        .upsert(as_caller(
            UpsertRequest {
                message_type: MSG.to_string(),
                record_json: serde_json::to_vec(&json!({
                    "id": id, "tenant_id": tenant, "project_id": other_project, "status": "PWNED"
                }))
                .unwrap(),
                ..UpsertRequest::default()
            },
            &tenant,
            &other_project,
            "svc-a1",
        ))
        .await;
    let err = result.expect_err("a cross-project upsert must be refused, not reported as success");
    assert!(
        matches!(
            err.code(),
            Code::FailedPrecondition | Code::PermissionDenied | Code::InvalidArgument
        ),
        "{err:?}"
    );
    let row: (String, String) = sqlx::query_as(&format!(
        "SELECT project_id, status FROM \"{schema}\".pwidgets WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await
    .expect("raw project-scoped read");
    assert_eq!(
        row,
        (DEFAULT_PROJECT_ID.to_string(), "ORIGINAL".to_string()),
        "the row must keep its project and values"
    );

    teardown(&pool, &schema, &tenant).await;
}

/// A5 — keyset pagination over a NULLABLE sort key walks every row exactly
/// once, including the NULL-keyed rows and a page boundary that lands on NULL.
///
/// Revert-proof: drop the NULL-aware cursor predicate and a full page ending on
/// a NULL key mints no cursor (the walk stops early), or the ascending walk
/// never reaches the NULL rows — the collected set is short.
#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn served_pagination_walks_null_sort_keys_exactly_once_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    let statuses = [Some("a"), None, Some("b"), None, None, Some("c"), None];
    let mut expected = Vec::new();
    for (index, status) in statuses.iter().enumerate() {
        let id = format!("p{index}-{}", Uuid::new_v4().simple());
        sqlx::query(&format!(
            "INSERT INTO \"{schema}\".widgets (id, tenant_id, status) VALUES ($1, $2, $3)"
        ))
        .bind(&id)
        .bind(&tenant)
        .bind(*status)
        .execute(&pool)
        .await
        .expect("seed pagination row");
        expected.push(id);
    }
    expected.sort();
    const MSG: &str = "acme.dp.v1.Widget";
    let svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;

    for descending in [false, true] {
        let mut seen = Vec::new();
        let mut page_token = String::new();
        for _ in 0..statuses.len() + 2 {
            let page = svc
                .select(with_ctx(
                    SelectRequest {
                        message_type: MSG.to_string(),
                        filter: json_to_struct(&json!({"tenant_id": tenant})),
                        sort: vec![Sort {
                            field: "status".to_string(),
                            descending,
                        }],
                        limit: 2,
                        page_token: page_token.clone(),
                        cache: Some(crate::proto::CacheOptions {
                            bypass_read: true,
                            bypass_write: true,
                            ..Default::default()
                        }),
                        ..SelectRequest::default()
                    },
                    &tenant,
                ))
                .await
                .expect("served paginated Select")
                .into_inner();
            for bytes in &page.records_json {
                let row: serde_json::Value =
                    serde_json::from_slice(bytes).expect("decode paginated row");
                seen.push(row["id"].as_str().expect("row id").to_string());
            }
            if page.next_page_token.is_empty() {
                break;
            }
            page_token = page.next_page_token;
        }
        let total = seen.len();
        seen.sort();
        seen.dedup();
        assert_eq!(
            seen.len(),
            total,
            "descending={descending}: a row was returned twice"
        );
        assert_eq!(
            seen, expected,
            "descending={descending}: the walk must reach every row, NULL keys included"
        );
    }

    teardown(&pool, &schema, &tenant).await;
}

/// A9 — a ciphertext-shaped value for an encrypted column and any value for
/// its blind-index (`_idx`) sibling are refused on the unary Upsert AND inside
/// BeginTx; nothing is written either way.
///
/// Revert-proof: drop `validate_client_encrypted_write` from the upsert path
/// (or the BeginTx upsert arm) and the planted ciphertext / chosen index token
/// is stored verbatim — the raw count becomes 1.
#[tokio::test]
#[ignore = "requires live Postgres + UDB_ENCRYPTION_KEY; run in the CI live lane"]
async fn served_ciphertext_and_blind_index_inputs_are_refused_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".vaulted \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, secret TEXT, secret_idx TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create vaulted");
    const MSG: &str = "acme.dp.v1.Vaulted";
    let mut secret = col("secret", "TEXT", false);
    secret.encrypted = true;
    let mut secret_idx = col("secret_idx", "TEXT", false);
    secret_idx.security.is_blind_index = true;
    let manifest = table_manifest(
        &schema,
        "vaulted",
        "Vaulted",
        vec![
            col("id", "TEXT", true),
            col("tenant_id", "TEXT", false),
            secret,
            secret_idx,
        ],
        &["id"],
        "",
    );
    let svc = dp_service(&dsn, manifest.clone()).await;
    let (mut client, shutdown, handle) = serve_data_broker(dp_service(&dsn, manifest).await).await;

    for (label, extra) in [
        (
            "ciphertext-shaped",
            json!({"secret": "udb-aead:v1:planted-ciphertext"}),
        ),
        (
            "blind-index",
            json!({"secret": "plain", "secret_idx": "caller-chosen-token"}),
        ),
    ] {
        let id = format!("a9-{}", Uuid::new_v4().simple());
        let mut record = json!({"id": id, "tenant_id": tenant});
        let fields = record.as_object_mut().expect("record object");
        for (key, value) in extra.as_object().expect("extra fields") {
            fields.insert(key.clone(), value.clone());
        }
        let err = served_upsert(&svc, &tenant, MSG, record.clone(), "")
            .await
            .expect_err("the unary Upsert must refuse it");
        assert_eq!(err.code(), Code::InvalidArgument, "{label}: {err:?}");

        let (committed, error) = begin_tx_outcome(
            &mut client,
            with_ctx(stream::iter(vec![upsert_mutation(MSG, record)]), &tenant),
        )
        .await;
        assert!(!committed, "{label}: the BeginTx upsert must not commit");
        assert!(error.is_some(), "{label}: BeginTx must surface the refusal");
        assert_eq!(
            raw_count(&pool, &schema, "vaulted", &id).await,
            0,
            "{label}: nothing may be written"
        );
    }

    let _ = shutdown.send(());
    let _ = handle.await;
    teardown(&pool, &schema, &tenant).await;
}

/// A4 — a BeginTx relational mutation carrying `idempotency_key` is refused
/// with InvalidArgument before the transaction opens, and nothing is written
/// (it used to be accepted and silently ignored).
#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn served_begin_tx_relational_idempotency_key_is_refused_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let (mut client, shutdown, handle) = serve_data_broker(
        dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await,
    )
    .await;

    let id = format!("a4-{}", Uuid::new_v4().simple());
    let mut mutation = upsert_mutation(MSG, json!({"id": id, "tenant_id": tenant, "status": "X"}));
    mutation.idempotency_key = format!("retry-{}", Uuid::new_v4().simple());
    let (committed, error) =
        begin_tx_outcome(&mut client, with_ctx(stream::iter(vec![mutation]), &tenant)).await;
    assert!(!committed, "the keyed relational mutation must not commit");
    let error = error.expect("BeginTx must refuse the relational idempotency_key");
    assert_eq!(error.code(), Code::InvalidArgument, "{error:?}");
    assert!(error.message().contains("idempotency_key"), "{error:?}");
    assert_eq!(raw_count(&pool, &schema, "widgets", &id).await, 0);

    let _ = shutdown.send(());
    let _ = handle.await;
    teardown(&pool, &schema, &tenant).await;
}

// ── B2 / B4 / B11 on the production (deny-by-default) posture ───────────────

const SUBJECT: &str = "svc-dp-seam";

/// Seed one row with a raw privileged INSERT (the deny broker grants no write
/// to the seeding identity on purpose).
async fn raw_seed_widget(pool: &sqlx::PgPool, schema: &str, table: &str, id: &str, tenant: &str) {
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".\"{table}\" (id, tenant_id, status) VALUES ($1, $2, 'SEEDED')"
    ))
    .bind(id)
    .bind(tenant)
    .execute(pool)
    .await
    .expect("raw seed row");
}

fn select_by_id(message: &str, id: &str, tenant: &str) -> SelectRequest {
    SelectRequest {
        message_type: message.to_string(),
        filter: json_to_struct(&json!({"id": id, "tenant_id": tenant})),
        ..SelectRequest::default()
    }
}

/// Two tenant-scoped tables (`widgets` / `gadgets`) in one manifest.
fn two_table_manifest(schema: &str) -> CatalogManifest {
    let mut manifest = widget_manifest(schema, "widgets", "Widget", false);
    manifest
        .tables
        .extend(widget_manifest(schema, "gadgets", "Gadget", false).tables);
    manifest
}

async fn create_two_tables(pool: &sqlx::PgPool, schema: &str) {
    create_schema(pool, schema).await;
    for table in ["widgets", "gadgets"] {
        sqlx::query(&format!(
            "CREATE TABLE \"{schema}\".{table} \
             (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
        ))
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("create {table}: {err}"));
    }
}

/// B11 — a NARROW policy served end to end: the one granted (subject, tenant,
/// project, table, action) reads its row; another action, table, tenant or
/// subject is PermissionDenied, and a denied write leaves no row.
///
/// Revert-proof: build the broker with default-allow ON (the old harness) and
/// every denied call succeeds.
#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn served_narrow_policy_is_enforced_as_narrow_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    dp_prepare_deny_db(&pool).await;

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let foreign_tenant = Uuid::new_v4().to_string();
    create_two_tables(&pool, &schema).await;
    const WIDGET: &str = "acme.dp.v1.Widget";
    const GADGET: &str = "acme.dp.v1.Gadget";
    let row_id = format!("b11-{}", Uuid::new_v4().simple());
    raw_seed_widget(&pool, &schema, "widgets", &row_id, &tenant).await;
    raw_seed_widget(&pool, &schema, "gadgets", &row_id, &tenant).await;
    raw_seed_widget(
        &pool,
        &schema,
        "widgets",
        &format!("{row_id}-f"),
        &foreign_tenant,
    )
    .await;

    let policy = dp_insert_allow_rule(
        &pool,
        &tenant,
        DEFAULT_PROJECT_ID,
        SUBJECT,
        WIDGET,
        "Select",
    )
    .await;
    let svc = dp_service_deny(&dsn, two_table_manifest(&schema)).await;
    dp_warm_authz(&svc, &[policy.as_str()]).await;

    // The narrow grant reads its own row.
    let rows = svc
        .select(as_caller(
            select_by_id(WIDGET, &row_id, &tenant),
            &tenant,
            DEFAULT_PROJECT_ID,
            SUBJECT,
        ))
        .await
        .expect("the narrow grant must allow exactly its own tuple")
        .into_inner();
    assert_eq!(
        rows.records_json.len(),
        1,
        "the granted read returns the row"
    );

    // Another action: denied, and nothing written.
    let new_id = format!("b11-new-{}", Uuid::new_v4().simple());
    let err = svc
        .upsert(as_caller(
            UpsertRequest {
                message_type: WIDGET.to_string(),
                record_json: serde_json::to_vec(
                    &json!({"id": new_id, "tenant_id": tenant, "status": "WRITTEN"}),
                )
                .unwrap(),
                ..UpsertRequest::default()
            },
            &tenant,
            DEFAULT_PROJECT_ID,
            SUBJECT,
        ))
        .await
        .expect_err("Upsert is outside the grant");
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    assert_eq!(raw_count(&pool, &schema, "widgets", &new_id).await, 0);

    // Another table / tenant / subject: denied.
    for (label, request) in [
        (
            "another table",
            as_caller(
                select_by_id(GADGET, &row_id, &tenant),
                &tenant,
                DEFAULT_PROJECT_ID,
                SUBJECT,
            ),
        ),
        (
            "another tenant",
            as_caller(
                select_by_id(WIDGET, &format!("{row_id}-f"), &foreign_tenant),
                &foreign_tenant,
                DEFAULT_PROJECT_ID,
                SUBJECT,
            ),
        ),
        (
            "another subject",
            as_caller(
                select_by_id(WIDGET, &row_id, &tenant),
                &tenant,
                DEFAULT_PROJECT_ID,
                "svc-dp-seam-intruder",
            ),
        ),
    ] {
        let err = svc
            .select(request)
            .await
            .expect_err("outside the narrow grant");
        assert_eq!(err.code(), Code::PermissionDenied, "{label}: {err:?}");
    }

    teardown(&pool, &schema, &tenant).await;
    teardown(&pool, &schema, &foreign_tenant).await;
}

/// B4 — an allow rule bound to the default project does not reach a second
/// project: the same subject/tenant/table/action is allowed in `default` and
/// PermissionDenied in the other ACTIVE project.
///
/// Revert-proof: drop the project pre-filter from the Casbin allow path and the
/// second-project Select is allowed.
#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn served_allow_rule_is_bound_to_its_project_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    dp_prepare_deny_db(&pool).await;

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let other_project = format!("b4-other-{}", Uuid::new_v4().simple());
    create_two_tables(&pool, &schema).await;
    const WIDGET: &str = "acme.dp.v1.Widget";
    let row_id = format!("b4-{}", Uuid::new_v4().simple());
    raw_seed_widget(&pool, &schema, "widgets", &row_id, &tenant).await;

    let policy = dp_insert_allow_rule(
        &pool,
        &tenant,
        DEFAULT_PROJECT_ID,
        SUBJECT,
        WIDGET,
        "Select",
    )
    .await;
    let svc = dp_service_deny(&dsn, two_table_manifest(&schema)).await;
    activate_second_project(&svc, &other_project).await;
    dp_warm_authz(&svc, &[policy.as_str()]).await;

    svc.select(as_caller(
        select_by_id(WIDGET, &row_id, &tenant),
        &tenant,
        DEFAULT_PROJECT_ID,
        SUBJECT,
    ))
    .await
    .expect("the default-project grant allows the default project");
    let err = svc
        .select(as_caller(
            select_by_id(WIDGET, &row_id, &tenant),
            &tenant,
            &other_project,
            SUBJECT,
        ))
        .await
        .expect_err("a default-project allow must not reach another project");
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");

    teardown(&pool, &schema, &tenant).await;
}

/// B2 — BeginTx authorizes EVERY mutation before applying any: a transaction
/// of [granted upsert, ungranted upsert] is PermissionDenied and the granted
/// half is not committed. PutObject to an ungranted bucket is PermissionDenied
/// before any byte reaches the store.
///
/// Revert-proof: authorize only the BeginTx control gate (not each mutation)
/// and the transaction commits both rows.
#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn served_begin_tx_and_put_object_authorize_every_target_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    dp_prepare_deny_db(&pool).await;

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_two_tables(&pool, &schema).await;
    const WIDGET: &str = "acme.dp.v1.Widget";
    const GADGET: &str = "acme.dp.v1.Gadget";
    let upsert_policy = dp_insert_allow_rule(
        &pool,
        &tenant,
        DEFAULT_PROJECT_ID,
        SUBJECT,
        WIDGET,
        "Upsert",
    )
    .await;
    let svc = dp_service_deny(&dsn, two_table_manifest(&schema)).await;
    dp_warm_authz(&svc, &[upsert_policy.as_str()]).await;
    let (mut client, shutdown, handle) = serve_data_broker(svc).await;

    let granted_id = format!("b2-granted-{}", Uuid::new_v4().simple());
    let ungranted_id = format!("b2-ungranted-{}", Uuid::new_v4().simple());
    let mutations = vec![
        upsert_mutation(
            WIDGET,
            json!({"id": granted_id, "tenant_id": tenant, "status": "GRANTED"}),
        ),
        upsert_mutation(
            GADGET,
            json!({"id": ungranted_id, "tenant_id": tenant, "status": "UNGRANTED"}),
        ),
    ];
    let (committed, error) = begin_tx_outcome(
        &mut client,
        as_caller(
            stream::iter(mutations),
            &tenant,
            DEFAULT_PROJECT_ID,
            SUBJECT,
        ),
    )
    .await;
    assert!(
        !committed,
        "a transaction with an ungranted mutation must not commit"
    );
    let error = error.expect("BeginTx must refuse the ungranted mutation");
    assert_eq!(error.code(), Code::PermissionDenied, "{error:?}");
    assert_eq!(
        raw_count(&pool, &schema, "widgets", &granted_id).await,
        0,
        "the granted half must not be committed"
    );
    assert_eq!(raw_count(&pool, &schema, "gadgets", &ungranted_id).await, 0);

    let chunks = vec![Chunk {
        bucket: format!("b2-ungranted-{}", Uuid::new_v4().simple()),
        object_key: "b2/object.bin".to_string(),
        data: b"never stored".to_vec(),
        final_chunk: true,
        content_type: "application/octet-stream".to_string(),
        ..Chunk::default()
    }];
    let err = client
        .put_object(as_caller(
            stream::iter(chunks),
            &tenant,
            DEFAULT_PROJECT_ID,
            SUBJECT,
        ))
        .await
        .expect_err("PutObject to an ungranted bucket must be refused");
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");

    let _ = shutdown.send(());
    let _ = handle.await;
    teardown(&pool, &schema, &tenant).await;
}
