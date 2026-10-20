//! Live seams for the journal-reader gates of the embedding work emitter and
//! the search freshness consumer against a real Postgres.
//!
//! With nothing registered that a journal event could feed (no ACTIVE embedding
//! source naming a CDC topic, no ACTIVE search index naming one) a pass must not
//! read the journal at all: it completes while another session holds an
//! exclusive lock on the journal, and its reader cursor moves to the head.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::support::{
    live_native_service_db_lock, live_pg_pool, migrate_native_service_db, require_live_dsn_any,
};
use crate::runtime::cdc::journal;
use crate::runtime::service::embedding_service::{
    EMBEDDING_JOURNAL_READER, EMBEDDING_WORK_EMITTER_BATCH, EmbeddingServiceImpl,
    emit_source_change_work,
};
use crate::runtime::service::search_service::{
    SEARCH_FRESHNESS_BATCH, SEARCH_FRESHNESS_READER, SearchServiceImpl,
    run_index_freshness_consumer,
};
use crate::runtime::system::SystemCatalogConfig;

const LIVE_DSNS: [&str; 3] = [
    "UDB_LIVE_NATIVE_PG_DSN",
    "UDB_LIVE_AUTH_PG_DSN",
    "UDB_INTEGRATION_PG_DSN",
];

/// Hold an ACCESS EXCLUSIVE lock on the journal in a transaction the caller
/// rolls back. Any journal read by the pass under test then blocks.
async fn lock_journal(
    pool: &sqlx::PgPool,
    journal: &str,
) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut holder = pool.begin().await.expect("lock-holding transaction");
    sqlx::query(&format!("LOCK TABLE {journal} IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *holder)
        .await
        .expect("hold the journal lock");
    holder
}

#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live lane"]
async fn embedding_emitter_without_sources_skips_the_journal_live() {
    if require_live_dsn_any(&LIVE_DSNS).is_none() {
        eprintln!("live Postgres DSN unset — skipping embedding emitter journal skip");
        return;
    }
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    // A freshly migrated database has no embedding source.
    migrate_native_service_db(&pool).await;

    let catalog = SystemCatalogConfig::current();
    let journal_relation = catalog.cdc_journal_relation();
    let outbox_relation = catalog.cdc.outbox_relation();
    let service = EmbeddingServiceImpl::new()
        .with_postgres(Some(pool.clone()))
        .with_outbox(Some(outbox_relation.clone()));

    let holder = lock_journal(&pool, &journal_relation).await;
    let head = journal::head(&pool, &catalog).await.expect("journal head");
    let started = Instant::now();
    let ticked = tokio::time::timeout(
        Duration::from_secs(2),
        emit_source_change_work(
            &service,
            &pool,
            &journal_relation,
            &outbox_relation,
            EMBEDDING_WORK_EMITTER_BATCH,
        ),
    )
    .await;
    let elapsed = started.elapsed();
    holder.rollback().await.expect("release the journal lock");
    assert_eq!(
        ticked
            .expect("a pass with no sources must not wait on the journal")
            .expect("embedding emitter pass"),
        0
    );
    assert!(elapsed < Duration::from_secs(2), "pass took {elapsed:?}");
    let reader = journal::reader_position(&pool, &catalog, EMBEDDING_JOURNAL_READER)
        .await
        .expect("embedding journal reader")
        .expect("the reader is recorded");
    assert!(
        reader >= head,
        "the reader moves to the head without reading"
    );
}

#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live lane"]
async fn search_freshness_without_indexes_skips_the_journal_live() {
    if require_live_dsn_any(&LIVE_DSNS).is_none() {
        eprintln!("live Postgres DSN unset — skipping search freshness journal skip");
        return;
    }
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    // A freshly migrated database has no search index.
    migrate_native_service_db(&pool).await;

    let catalog = SystemCatalogConfig::current();
    let journal_relation = catalog.cdc_journal_relation();
    let service = Arc::new(
        SearchServiceImpl::new()
            .with_postgres(Some(pool.clone()))
            .with_outbox(Some(catalog.cdc.outbox_relation())),
    );

    let holder = lock_journal(&pool, &journal_relation).await;
    let head = journal::head(&pool, &catalog).await.expect("journal head");
    let started = Instant::now();
    let ticked = tokio::time::timeout(
        Duration::from_secs(2),
        run_index_freshness_consumer(service, &journal_relation, SEARCH_FRESHNESS_BATCH),
    )
    .await;
    let elapsed = started.elapsed();
    holder.rollback().await.expect("release the journal lock");
    assert_eq!(
        ticked
            .expect("a pass with no indexes must not wait on the journal")
            .expect("search freshness pass"),
        0
    );
    assert!(elapsed < Duration::from_secs(2), "pass took {elapsed:?}");
    let reader = journal::reader_position(&pool, &catalog, SEARCH_FRESHNESS_READER)
        .await
        .expect("search freshness reader")
        .expect("the reader is recorded");
    assert!(
        reader >= head,
        "the reader moves to the head without reading"
    );
}
