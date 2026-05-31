//! U4 acceptance: project isolation end-to-end.
//!
//! Builds two manifests in one `CatalogManager`, then proves that
//! resolving by `project_id` hands back each project's own tables — a
//! direct check that `CatalogManager::active_for` is the right seam for
//! the data-path handlers. The handlers themselves (`handlers_data.rs`
//! et al.) already use this seam, so a green test here means a request
//! tagged for project A can't accidentally see project B's manifest.

#![cfg(test)]

use std::sync::Arc;

use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::runtime::catalog::{CatalogManager, DEFAULT_PROJECT_ID};

fn manifest_for(message_name: &str, schema: &str, table: &str, checksum: &str) -> CatalogManifest {
    CatalogManifest {
        checksum_sha256: checksum.to_string(),
        generator_version: "u4-test".to_string(),
        tables: vec![ManifestTable {
            checksum_sha256: checksum.to_string(),
            message_name: message_name.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![ManifestColumn {
                field_name: "id".into(),
                column_name: "id".into(),
                proto_type: "string".into(),
                sql_type: "uuid".into(),
                is_primary: true,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn make_manager_with_two_projects() -> Arc<CatalogManager> {
    let initial = manifest_for(
        "default.system.v1.Heartbeat",
        "system",
        "heartbeats",
        "default-initial",
    );
    let manager = Arc::new(CatalogManager::new(initial));

    // Project "billing" owns Customer; project "analytics" owns Event.
    let billing = manifest_for(
        "acme.billing.v1.Customer",
        "billing",
        "customers",
        "billing-checksum",
    );
    manager
        .stage_catalog(billing, "billing".into(), "1.0.0".into(), "backward".into())
        .await
        .expect("stage billing");
    manager
        .activate_catalog_for("billing", "1.0.0")
        .await
        .expect("activate billing");

    let analytics = manifest_for(
        "acme.analytics.v1.Event",
        "analytics",
        "events",
        "analytics-checksum",
    );
    manager
        .stage_catalog(
            analytics,
            "analytics".into(),
            "1.0.0".into(),
            "backward".into(),
        )
        .await
        .expect("stage analytics");
    manager
        .activate_catalog_for("analytics", "1.0.0")
        .await
        .expect("activate analytics");

    manager
}

/// **U4 acceptance gate**: "One broker process serves two projects with
/// different proto catalogs and **no message type collision**." Each
/// project's active manifest lists only its own messages.
#[tokio::test]
async fn two_projects_serve_distinct_manifests() {
    let manager = make_manager_with_two_projects().await;

    let billing = manager.active_for("billing");
    let analytics = manager.active_for("analytics");

    let billing_tables: Vec<&str> = billing
        .manifest
        .tables
        .iter()
        .map(|t| t.message_name.as_str())
        .collect();
    let analytics_tables: Vec<&str> = analytics
        .manifest
        .tables
        .iter()
        .map(|t| t.message_name.as_str())
        .collect();

    assert_eq!(billing_tables, vec!["acme.billing.v1.Customer"]);
    assert_eq!(analytics_tables, vec!["acme.analytics.v1.Event"]);
    assert_eq!(billing.metadata.checksum, "billing-checksum");
    assert_eq!(analytics.metadata.checksum, "analytics-checksum");
}

/// **U4 acceptance gate**: "Project A cannot query Project B message
/// types." A lookup for an analytics-only message against the billing
/// manifest must return None.
#[tokio::test]
async fn project_a_manifest_does_not_expose_project_b_messages() {
    let manager = make_manager_with_two_projects().await;
    let billing = manager.active_for("billing");

    // The lookup `table_for_message` is what every compiler / handler
    // uses. Looking up an analytics message against the billing manifest
    // must miss — that's exactly what stops cross-project leakage.
    let leak_attempt =
        crate::broker::table_for_message(&billing.manifest, "acme.analytics.v1.Event");
    assert!(
        leak_attempt.is_none(),
        "billing manifest must NOT see analytics messages, but found {:?}",
        leak_attempt.map(|t| t.message_name.clone())
    );

    // Sanity: the same lookup against the analytics manifest finds it.
    let analytics = manager.active_for("analytics");
    let found = crate::broker::table_for_message(&analytics.manifest, "acme.analytics.v1.Event");
    assert!(
        found.is_some(),
        "analytics manifest must serve its own messages"
    );
}

/// **U4 acceptance gate**: "Catalog activation for Project A does not
/// mutate Project B routing." Activating a new billing catalog must
/// leave analytics' manifest untouched.
#[tokio::test]
async fn activating_one_project_does_not_disturb_another() {
    let manager = make_manager_with_two_projects().await;

    // Snapshot the analytics checksum before the billing rotation.
    let analytics_before = manager.active_for("analytics").metadata.checksum.clone();
    assert_eq!(analytics_before, "analytics-checksum");

    // Stage and activate a NEW billing catalog. Analytics must not move.
    let new_billing = manifest_for(
        "acme.billing.v1.Invoice",
        "billing",
        "invoices",
        "billing-v2",
    );
    manager
        .stage_catalog(
            new_billing,
            "billing".into(),
            "2.0.0".into(),
            "backward".into(),
        )
        .await
        .unwrap();
    manager
        .activate_catalog_for("billing", "2.0.0")
        .await
        .unwrap();

    assert_eq!(
        manager.active_for("billing").metadata.checksum,
        "billing-v2"
    );
    assert_eq!(
        manager.active_for("analytics").metadata.checksum,
        analytics_before,
        "analytics catalog must be untouched by billing rotation"
    );

    // The old billing Customer message is GONE from the billing
    // manifest now (the new manifest only declares Invoice). Analytics
    // still has Event.
    let billing = manager.active_for("billing");
    assert!(
        crate::broker::table_for_message(&billing.manifest, "acme.billing.v1.Customer").is_none(),
        "old billing message must be gone after rotation"
    );
    assert!(
        crate::broker::table_for_message(&billing.manifest, "acme.billing.v1.Invoice").is_some(),
        "new billing message must be present after rotation"
    );

    let analytics = manager.active_for("analytics");
    assert!(
        crate::broker::table_for_message(&analytics.manifest, "acme.analytics.v1.Event").is_some(),
        "analytics message must be untouched"
    );
}

/// Backward-compat: a request with no `project_id` (empty string) falls
/// back to the default project's catalog — not to whichever project was
/// staged last, and certainly not to billing or analytics.
#[tokio::test]
async fn blank_project_id_resolves_to_default_not_any_other_project() {
    let manager = make_manager_with_two_projects().await;

    let blank = manager.active_for("");
    let default = manager.active_for(DEFAULT_PROJECT_ID);
    assert_eq!(blank.metadata.checksum, "default-initial");
    assert_eq!(default.metadata.checksum, "default-initial");

    // Critical: the blank lookup must NOT hand back the billing or
    // analytics catalog even though those exist and were staged later.
    assert_ne!(blank.metadata.checksum, "billing-checksum");
    assert_ne!(blank.metadata.checksum, "analytics-checksum");
}

/// Per-project staging is independent — staging for billing doesn't make
/// the analytics staging slot visible, and vice versa.
#[tokio::test]
async fn staging_slots_are_per_project() {
    let manager = make_manager_with_two_projects().await;

    let pending_billing = manifest_for(
        "acme.billing.v1.PendingInvoice",
        "billing",
        "pending_invoices",
        "billing-pending",
    );
    manager
        .stage_catalog(
            pending_billing,
            "billing".into(),
            "3.0.0".into(),
            "backward".into(),
        )
        .await
        .unwrap();

    // Only billing has a staged catalog now.
    assert!(manager.staged_for("billing").await.is_some());
    assert!(manager.staged_for("analytics").await.is_none());
    assert!(manager.staged_for(DEFAULT_PROJECT_ID).await.is_none());

    // Rolling back billing must NOT affect analytics' active or staged
    // state, even though they share the same CatalogManager.
    manager.rollback_catalog_for("billing").await.unwrap();
    assert!(manager.staged_for("billing").await.is_none());
    assert_eq!(
        manager.active_for("analytics").metadata.checksum,
        "analytics-checksum"
    );
}
