//! Schema changes UDB used to refuse, applied to a live Postgres: each renders from a real
//! manifest diff, runs as the migration runs it, and the database is read back.

use uuid::Uuid;

use super::data_plane_live::{col, create_schema, dp_live_pg_dsn, dp_pool};
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::migration::diff::{ChangeKind, diff_manifests};

struct Fx {
    pool: sqlx::PgPool,
    schema: String,
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl Fx {
    async fn new(ddl: &str) -> Option<Self> {
        let dsn = dp_live_pg_dsn()?;
        let guard = super::support::live_native_service_db_lock().lock().await;
        let pool = dp_pool(&dsn).await;
        let schema = format!("udb_mig_{}", Uuid::new_v4().simple());
        create_schema(&pool, &schema).await;
        sqlx::raw_sql(&ddl.replace("{schema}", &schema))
            .execute(&pool)
            .await
            .expect("the starting table");
        Some(Self {
            pool,
            schema,
            _guard: guard,
        })
    }

    fn table(&self, primary_key: &[&str], columns: Vec<ManifestColumn>) -> ManifestTable {
        ManifestTable {
            proto_package: "acme.mig.v1".to_string(),
            message_name: "Row".to_string(),
            schema: self.schema.clone(),
            table: "rows".to_string(),
            primary_key: primary_key.iter().map(ToString::to_string).collect(),
            columns,
            ..ManifestTable::default()
        }
    }

    /// Diff the two tables and run the delta of `kind` in one transaction, as the migration does.
    async fn apply(&self, old: ManifestTable, new: ManifestTable, kind: ChangeKind) {
        let manifest = |table: ManifestTable, checksum: &str| CatalogManifest {
            tables: vec![table],
            checksum_sha256: checksum.to_string(),
            ..CatalogManifest::default()
        };
        let old = manifest(old, "old");
        let new = manifest(new, "new");
        let mut old_changed = old.clone();
        old_changed.tables[0].checksum_sha256 = "a".to_string();
        let mut new_changed = new.clone();
        new_changed.tables[0].checksum_sha256 = "b".to_string();
        let changes = diff_manifests(Some(&old_changed), &new_changed);
        let change = changes
            .iter()
            .find(|change| change.kind == kind)
            .unwrap_or_else(|| panic!("no {kind:?} in {changes:?}"));
        let sql = crate::generation::sql::render_delta_operation(
            &new_changed,
            new_changed.tables.first(),
            change,
        );
        assert!(!sql.trim().is_empty(), "{kind:?} renders nothing");
        sqlx::raw_sql(&format!("BEGIN;\n{sql}\nCOMMIT;"))
            .execute(&self.pool)
            .await
            .unwrap_or_else(|err| panic!("{kind:?} failed: {err}\n{sql}"));
    }

    async fn text(&self, sql: &str) -> Option<String> {
        sqlx::query_scalar(&sql.replace("{schema}", &self.schema))
            .fetch_one(&self.pool)
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err}"))
    }

    async fn finish(self) {
        let _ = sqlx::raw_sql(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .execute(&self.pool)
            .await;
    }
}

macro_rules! live {
    ($name:ident, $body:block) => {
        #[tokio::test]
        #[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
        async fn $name() $body
    };
}

macro_rules! fixture {
    ($ddl:expr) => {
        match Fx::new($ddl).await {
            Some(fx) => fx,
            None => return,
        }
    };
}

live!(a_changed_primary_key_is_applied_live, {
    let fx = fixture!(
        "CREATE TABLE \"{schema}\".rows (id TEXT PRIMARY KEY, k TEXT NOT NULL); \
         INSERT INTO \"{schema}\".rows VALUES ('a', 'x')"
    );
    let columns = vec![col("id", "TEXT", true), col("k", "TEXT", true)];
    let old = fx.table(&["id"], columns.clone());
    let new = fx.table(&["id", "k"], columns);
    fx.apply(old, new, ChangeKind::ChangePrimaryKey).await;
    assert_eq!(
        fx.text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid = '\"{schema}\".rows'::regclass AND contype = 'p'"
        )
        .await
        .as_deref(),
        Some("PRIMARY KEY (id, k)")
    );
    fx.finish().await;
});

live!(a_primary_key_the_rows_break_stops_the_migration_live, {
    let fx = fixture!(
        "CREATE TABLE \"{schema}\".rows (id TEXT PRIMARY KEY, k TEXT NOT NULL); \
         INSERT INTO \"{schema}\".rows VALUES ('a', 'same'), ('b', 'same')"
    );
    let columns = vec![col("id", "TEXT", false), col("k", "TEXT", true)];
    let old = fx.table(&["id"], columns.clone());
    let new = fx.table(&["k"], columns);
    let changes = {
        let manifest = |mut table: ManifestTable, sum: &str| {
            table.checksum_sha256 = sum.to_string();
            CatalogManifest {
                tables: vec![table],
                checksum_sha256: sum.to_string(),
                ..CatalogManifest::default()
            }
        };
        diff_manifests(Some(&manifest(old, "o")), &manifest(new.clone(), "n"))
    };
    let change = changes
        .iter()
        .find(|change| change.kind == ChangeKind::ChangePrimaryKey)
        .expect("a primary key change");
    let holder = CatalogManifest {
        tables: vec![new],
        ..CatalogManifest::default()
    };
    let sql =
        crate::generation::sql::render_delta_operation(&holder, holder.tables.first(), change);
    let refused = sqlx::raw_sql(&format!("BEGIN;\n{sql}\nCOMMIT;"))
        .execute(&fx.pool)
        .await;
    assert!(refused.is_err(), "duplicate rows must stop it");
    let _ = sqlx::raw_sql("ROLLBACK").execute(&fx.pool).await;
    fx.finish().await;
});

live!(a_column_becomes_generated_and_changes_expression_live, {
    let fx = fixture!(
        "CREATE TABLE \"{schema}\".rows (id TEXT PRIMARY KEY, n INT, twice INT); \
         INSERT INTO \"{schema}\".rows (id, n, twice) VALUES ('a', 4, 0)"
    );
    let plain = vec![
        col("id", "TEXT", true),
        col("n", "INT", false),
        col("twice", "INT", false),
    ];
    let generated = |expr: &str| {
        let mut columns = plain.clone();
        columns[2].generated = true;
        columns[2].generated_expr = expr.to_string();
        columns
    };
    let old = fx.table(&["id"], plain.clone());
    let new = fx.table(&["id"], generated("n * 2"));
    fx.apply(old, new.clone(), ChangeKind::ChangeColumnGeneration)
        .await;
    assert_eq!(
        fx.text("SELECT twice::TEXT FROM \"{schema}\".rows")
            .await
            .as_deref(),
        Some("8")
    );
    let newer = fx.table(&["id"], generated("n * 3"));
    fx.apply(new, newer.clone(), ChangeKind::ChangeColumnGeneration)
        .await;
    assert_eq!(
        fx.text("SELECT twice::TEXT FROM \"{schema}\".rows")
            .await
            .as_deref(),
        Some("12")
    );
    // Leaving generation keeps the values.
    fx.apply(
        newer,
        fx.table(&["id"], plain.clone()),
        ChangeKind::ChangeColumnGeneration,
    )
    .await;
    sqlx::raw_sql(&format!(
        "UPDATE \"{}\".rows SET twice = twice + 1",
        fx.schema
    ))
    .execute(&fx.pool)
    .await
    .expect("a plain column is writable");
    assert_eq!(
        fx.text("SELECT twice::TEXT FROM \"{schema}\".rows")
            .await
            .as_deref(),
        Some("13")
    );
    fx.finish().await;
});

live!(
    a_column_becomes_identity_and_continues_from_its_rows_live,
    {
        let fx = fixture!(
            "CREATE TABLE \"{schema}\".rows (id BIGINT PRIMARY KEY, note TEXT); \
         INSERT INTO \"{schema}\".rows VALUES (41, 'seed')"
        );
        let plain = vec![col("id", "BIGINT", true), col("note", "TEXT", false)];
        let mut identity = plain.clone();
        identity[0].is_identity = true;
        fx.apply(
            fx.table(&["id"], plain),
            fx.table(&["id"], identity),
            ChangeKind::ChangeColumnGeneration,
        )
        .await;
        sqlx::raw_sql(&format!(
            "INSERT INTO \"{}\".rows (note) VALUES ('next')",
            fx.schema
        ))
        .execute(&fx.pool)
        .await
        .expect("identity fills the column");
        assert_eq!(
            fx.text("SELECT id::TEXT FROM \"{schema}\".rows WHERE note = 'next'")
                .await
                .as_deref(),
            Some("42")
        );
        fx.finish().await;
    }
);

live!(a_derived_index_is_rebuilt_live, {
    let fx = fixture!("CREATE TABLE \"{schema}\".rows (id TEXT PRIMARY KEY, doc JSONB)");
    let mut gin = col("doc", "JSONB", false);
    gin.is_jsonb = true;
    let mut path_ops = gin.clone();
    path_ops.json_path_ops = true;
    let mut plain_doc = gin.clone();
    plain_doc.is_jsonb = false;
    let id = col("id", "TEXT", true);
    fx.apply(
        fx.table(&["id"], vec![id.clone(), plain_doc]),
        fx.table(&["id"], vec![id.clone(), gin.clone()]),
        ChangeKind::RebuildDerivedIndex,
    )
    .await;
    fx.apply(
        fx.table(&["id"], vec![id.clone(), gin]),
        fx.table(&["id"], vec![id, path_ops]),
        ChangeKind::RebuildDerivedIndex,
    )
    .await;
    let definition = fx
        .text(
            "SELECT string_agg(indexdef, ' ') FROM pg_indexes \
             WHERE schemaname = '{schema}' AND indexname LIKE 'gin_%'",
        )
        .await
        .unwrap_or_default();
    assert!(definition.contains("jsonb_path_ops"), "{definition}");
    fx.finish().await;
});

fn partitioned(mut table: ManifestTable) -> ManifestTable {
    table.partition_strategy = "PARTITION_STRATEGY_RANGE_MONTH".to_string();
    table.partition_column = "created_at".to_string();
    table.partition_interval = "MONTHLY".to_string();
    table
}

live!(
    a_table_enters_partitioning_with_its_rows_and_keeps_the_old_one_live,
    {
        let fx = fixture!(
            "CREATE TABLE \"{schema}\".rows (id TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, \
         note TEXT, PRIMARY KEY (id, created_at)); \
         INSERT INTO \"{schema}\".rows VALUES ('now', NOW(), 'a'), ('old', '2001-01-01', 'b')"
        );
        let columns = vec![
            col("id", "TEXT", true),
            col("created_at", "TIMESTAMPTZ", true),
            col("note", "TEXT", false),
        ];
        let old = fx.table(&["id", "created_at"], columns.clone());
        let new = partitioned(fx.table(&["id", "created_at"], columns));
        fx.apply(old, new.clone(), ChangeKind::CreatePartition)
            .await;
        assert_eq!(
            fx.text(
                "SELECT partstrat::TEXT FROM pg_partitioned_table \
             WHERE partrelid = '\"{schema}\".rows'::regclass"
            )
            .await
            .as_deref(),
            Some("r")
        );
        assert_eq!(
            fx.text("SELECT count(*)::TEXT FROM \"{schema}\".rows")
                .await
                .as_deref(),
            Some("2"),
            "every row is copied"
        );
        assert_eq!(
            fx.text("SELECT count(*)::TEXT FROM \"{schema}\".rows__pre_repartition")
                .await
                .as_deref(),
            Some("2"),
            "the previous table is kept"
        );
        // And out again, under another option set.
        let mut options = new.clone();
        options.partition_premake = 2;
        fx.apply(new, options, ChangeKind::AttachPartition).await;
        fx.finish().await;
    }
);

live!(a_referenced_table_is_not_rebuilt_live, {
    let fx = fixture!(
        "CREATE TABLE \"{schema}\".rows (id TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, \
         PRIMARY KEY (id, created_at)); \
         CREATE TABLE \"{schema}\".child (id TEXT, created_at TIMESTAMPTZ, \
         FOREIGN KEY (id, created_at) REFERENCES \"{schema}\".rows (id, created_at))"
    );
    let columns = vec![
        col("id", "TEXT", true),
        col("created_at", "TIMESTAMPTZ", true),
    ];
    let table = partitioned(fx.table(&["id", "created_at"], columns));
    let sql = crate::generation::sql::render_table_rebuild(&table);
    let refused = sqlx::raw_sql(&format!("BEGIN;\n{sql}\nCOMMIT;"))
        .execute(&fx.pool)
        .await
        .expect_err("a referenced table is refused");
    assert!(
        refused.to_string().contains("referenced by a foreign key"),
        "{refused}"
    );
    let _ = sqlx::raw_sql("ROLLBACK").execute(&fx.pool).await;
    fx.finish().await;
});
