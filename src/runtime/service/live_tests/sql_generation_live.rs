//! Actual generated PostgreSQL uniqueness proof; the portable compiler stays runtime-free.

use crate::generation::sql::tests::partial_unique_fixture;
use crate::generation::sql::{SqlGenerationConfig, qi, render_bootstrap_table};

// Execute the actual generator's DDL, with both schema and rows owned by
// one rollback transaction. A failed assertion also rolls back its schema.
#[tokio::test]
#[ignore = "requires actual PostgreSQL; Native CI supplies UDB_INTEGRATION_PG_DSN"]
async fn unconditional_unique_survives_partial_index_live() {
    let dsn = std::env::var("UDB_INTEGRATION_PG_DSN")
        .or_else(|_| std::env::var("UDB_PG_DSN"))
        .expect("live uniqueness proof requires a configured PostgreSQL DSN");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&dsn)
        .await
        .unwrap_or_else(|_| panic!("connect configured live PostgreSQL"));
    let mut tx = pool
        .begin()
        .await
        .expect("begin owned uniqueness proof transaction");
    let schema = format!("ug2_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    for (table, unconditional) in [("partial", false), ("full", true)] {
        let manifest = partial_unique_fixture(&schema, table, unconditional);
        let ddl =
            render_bootstrap_table(&manifest, "live-fixture", &SqlGenerationConfig::default());
        sqlx::raw_sql(&ddl)
            .execute(&mut *tx)
            .await
            .expect("apply actual generated uniqueness DDL");
    }
    sqlx::raw_sql(&format!(
        "INSERT INTO {}.\"partial\" (\"key\", active) VALUES ('same', false), ('same', false)",
        qi(&schema)
    ))
    .execute(&mut *tx)
    .await
    .expect("partial-only index permits duplicate values outside its predicate");
    sqlx::raw_sql(&format!(
        "INSERT INTO {}.\"full\" (\"key\", active) VALUES ('same', false)",
        qi(&schema)
    ))
    .execute(&mut *tx)
    .await
    .expect("first full unique row succeeds outside partial predicate");
    let error = sqlx::raw_sql(&format!(
        "INSERT INTO {}.\"full\" (\"key\", active) VALUES ('same', false)",
        qi(&schema)
    ))
    .execute(&mut *tx)
    .await
    .expect_err("unconditional_unique_outside_partial_predicate must refuse the duplicate");
    let database = error
        .as_database_error()
        .expect("duplicate refusal is an actual database error");
    assert_eq!(
        database.code().as_deref(),
        Some("23505"),
        "duplicate must fail as unique_violation"
    );
    let constraint = format!("uidx_{schema}_full_key");
    assert_eq!(
        database.constraint(),
        Some(constraint.as_str()),
        "refusal must name the emitted unconditional constraint"
    );
    tx.rollback()
        .await
        .expect("rollback owned proof schema and rows");
    pool.close().await;
}
