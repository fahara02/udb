//! Live Postgres suite for the M5 surface of the native `MessagingService`: the
//! opt-in encrypted history backup RPCs and `PurgeSubject` coverage of the
//! messaging tables. Same harness as `live.rs` (a real tonic transport, OpenMLS
//! clients as devices).
//!
//!   UDB_LIVE_NATIVE_PG_DSN=postgres://udb:udb@127.0.0.1:55432/udb \
//!     cargo test --lib messaging_service::tests::live_backup -- --ignored --test-threads=1

use tonic::{Code, Request};

use crate::proto::PurgeSubjectRequest;
use crate::proto::data_broker_server::DataBroker;
use crate::proto::udb::core::messaging::services::v1 as msg_pb;
use crate::proto::udb::core::messaging::services::v1::messaging_service_server::MessagingService;
use crate::runtime::security::verify_erasure_receipt;
use crate::runtime::service::live_tests::support::{
    live_native_service_db_lock, live_pg_pool, messaging_service, migrate_native_service_db,
    purge_capable_broker_service,
};
use crate::runtime::service::method_security::{
    VerifiedClaimContext, scope_claim_context_for_test,
};

use super::super::config::{MAX_BACKUP_BYTES, MIN_BACKUP_BYTES};
use super::client::Device;
use super::live::{Client, claim, drain, publish, register, req, serve, submit};
use super::{decode_detail, envelope};

async fn put(
    client: &mut Client,
    tenant: &str,
    subject: &str,
    ciphertext: Vec<u8>,
    expected_version: i64,
) -> Result<msg_pb::PutMessagingBackupResponse, tonic::Status> {
    client
        .put_messaging_backup(req(
            tenant,
            msg_pb::PutMessagingBackupRequest {
                tenant_id: tenant.to_string(),
                subject_id: subject.to_string(),
                backup_id: String::new(),
                ciphertext,
                expected_version,
            },
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn get(
    client: &mut Client,
    tenant: &str,
    subject: &str,
) -> Result<msg_pb::GetMessagingBackupResponse, tonic::Status> {
    client
        .get_messaging_backup(req(
            tenant,
            msg_pb::GetMessagingBackupRequest {
                tenant_id: tenant.to_string(),
                subject_id: subject.to_string(),
                backup_id: String::new(),
            },
        ))
        .await
        .map(tonic::Response::into_inner)
}

fn as_user(tenant: &str, subject: &str) -> VerifiedClaimContext {
    VerifiedClaimContext {
        authenticated: true,
        subject: subject.to_string(),
        tenant_id: tenant.to_string(),
        ..Default::default()
    }
}

/// M5: a backup is stored as the opaque envelope it was sent as, overwrites
/// bump the version (compare-and-swap refuses a stale one with the current
/// version), oversized and non-envelope bodies are refused, another tenant
/// cannot see it (handlers and RLS), an end user can reach only its own, and
/// a delete is idempotent.
#[tokio::test]
#[ignore = "requires live Postgres; run with cargo test --lib messaging_service::tests::live_backup -- --ignored --test-threads=1"]
async fn live_messaging_backups_are_owner_scoped_bounded_and_tenant_isolated() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let (client, _shutdown) = serve().await;
    let mut c = client.clone();
    let tenant_a = uuid::Uuid::new_v4().to_string();
    let tenant_b = uuid::Uuid::new_v4().to_string();

    let first = envelope(4096);
    let stored = put(&mut c, &tenant_a, "alice", first.clone(), 0)
        .await
        .expect("store a backup");
    assert!(stored.created);
    let info = stored.backup.expect("backup info");
    assert_eq!((info.backup_id.as_str(), info.version), ("default", 1));
    assert_eq!(info.size_bytes, first.len() as i64);
    assert_eq!(
        info.ciphertext_sha256,
        super::super::model::envelope_digest(&first)
    );
    let read = get(&mut c, &tenant_a, "alice").await.expect("read it back");
    assert_eq!(read.ciphertext, first, "the envelope is stored verbatim");

    // Overwrites bump the version; a compare-and-swap on a stale version loses.
    let mut second = envelope(8192);
    second[100] = 0x5A;
    let replaced = put(&mut c, &tenant_a, "alice", second.clone(), 0)
        .await
        .expect("replace");
    assert!(!replaced.created);
    assert_eq!(replaced.backup.expect("info").version, 2);
    let stale = put(&mut c, &tenant_a, "alice", envelope(MIN_BACKUP_BYTES), 1)
        .await
        .expect_err("stale compare-and-swap");
    assert_eq!(stale.code(), Code::FailedPrecondition);
    assert_eq!(
        decode_detail(&stale)
            .missing
            .get("current_version")
            .map(String::as_str),
        Some("2")
    );
    let swapped = put(&mut c, &tenant_a, "alice", envelope(MIN_BACKUP_BYTES), 2)
        .await
        .expect("current compare-and-swap");
    assert_eq!(swapped.backup.expect("info").version, 3);
    let missing_cas = put(&mut c, &tenant_a, "carol", envelope(MIN_BACKUP_BYTES), 1)
        .await
        .expect_err("compare-and-swap on a missing backup");
    assert_eq!(
        decode_detail(&missing_cas)
            .missing
            .get("current_version")
            .map(String::as_str),
        Some("0")
    );

    // Size limit and envelope framing.
    for (body, why) in [
        (envelope(MAX_BACKUP_BYTES + 1), "oversized"),
        (
            b"plain history export, not sealed".repeat(8),
            "not an envelope",
        ),
    ] {
        let err = put(&mut c, &tenant_a, "alice", body, 0)
            .await
            .expect_err(why);
        assert_eq!(err.code(), Code::InvalidArgument, "{why}");
    }
    let largest = envelope(MAX_BACKUP_BYTES);
    put(&mut c, &tenant_a, "bob", largest.clone(), 0)
        .await
        .expect("the largest allowed backup round-trips");
    assert_eq!(
        get(&mut c, &tenant_a, "bob")
            .await
            .expect("read largest")
            .ciphertext
            .len(),
        largest.len()
    );

    // Tenant isolation through the handlers...
    let foreign = get(&mut c, &tenant_b, "alice")
        .await
        .expect_err("another tenant sees nothing");
    assert_eq!(foreign.code(), Code::NotFound);
    // ...and through Postgres RLS (a role that cannot bypass it).
    let role = format!("udb_messaging_rls_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE \"{role}\" NOLOGIN NOSUPERUSER NOBYPASSRLS; \
         GRANT USAGE ON SCHEMA udb_messaging TO \"{role}\"; \
         GRANT SELECT ON udb_messaging.backups TO \"{role}\""
    ))
    .execute(&pool)
    .await
    .expect("create RLS inspection role");
    let (enabled, forced): (bool, bool) = sqlx::query_as(
        "SELECT relrowsecurity, relforcerowsecurity FROM pg_class \
         WHERE oid = to_regclass('udb_messaging.backups')",
    )
    .fetch_one(&pool)
    .await
    .expect("RLS posture");
    assert!(enabled && forced, "udb_messaging.backups must force RLS");
    for (scope, expected) in [(tenant_a.as_str(), 2i64), (tenant_b.as_str(), 0)] {
        let mut tx = pool.begin().await.expect("begin RLS probe");
        sqlx::query(&format!("SET LOCAL ROLE \"{role}\""))
            .execute(&mut *tx)
            .await
            .expect("assume RLS role");
        sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
            .bind(scope)
            .execute(&mut *tx)
            .await
            .expect("tenant scope");
        let visible: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM udb_messaging.backups")
            .fetch_one(&mut *tx)
            .await
            .expect("RLS probe read");
        tx.rollback().await.expect("rollback RLS probe");
        assert_eq!(visible, expected, "tenant {scope}");
    }
    sqlx::raw_sql(&format!(
        "REVOKE ALL ON udb_messaging.backups FROM \"{role}\"; \
         REVOKE USAGE ON SCHEMA udb_messaging FROM \"{role}\"; DROP ROLE \"{role}\""
    ))
    .execute(&pool)
    .await
    .expect("drop RLS inspection role");

    // Owner-only: an end user reads its own backup (empty subject = "me") and
    // is refused another principal's.
    let svc = messaging_service().await;
    let mut own = Request::new(msg_pb::GetMessagingBackupRequest {
        tenant_id: tenant_a.clone(),
        subject_id: String::new(),
        backup_id: String::new(),
    });
    own.metadata_mut()
        .insert("x-tenant-id", tenant_a.parse().expect("tenant header"));
    let mine =
        scope_claim_context_for_test(as_user(&tenant_a, "bob"), svc.get_messaging_backup(own))
            .await
            .expect("bob reads his own backup")
            .into_inner();
    assert_eq!(mine.backup.expect("info").subject_id, "bob");
    for subject in ["alice", "carol"] {
        let mut theirs = Request::new(msg_pb::DeleteMessagingBackupRequest {
            tenant_id: tenant_a.clone(),
            subject_id: subject.to_string(),
            backup_id: String::new(),
        });
        theirs
            .metadata_mut()
            .insert("x-tenant-id", tenant_a.parse().expect("tenant header"));
        let refused = scope_claim_context_for_test(
            as_user(&tenant_a, "bob"),
            svc.delete_messaging_backup(theirs),
        )
        .await
        .expect_err("bob cannot delete another principal's backup");
        assert_eq!(refused.code(), Code::PermissionDenied);
    }
    get(&mut c, &tenant_a, "alice")
        .await
        .expect("alice's backup survived bob's attempts");

    // Delete is idempotent.
    for expect_deleted in [true, false] {
        let deleted = c
            .delete_messaging_backup(req(
                &tenant_a,
                msg_pb::DeleteMessagingBackupRequest {
                    tenant_id: tenant_a.clone(),
                    subject_id: "alice".to_string(),
                    backup_id: String::new(),
                },
            ))
            .await
            .expect("delete backup")
            .into_inner();
        assert_eq!(deleted.deleted, expect_deleted);
    }
    assert_eq!(
        get(&mut c, &tenant_a, "alice")
            .await
            .expect_err("gone")
            .code(),
        Code::NotFound
    );
}

/// M5: `PurgeSubject` erases the subject's messaging devices, KeyPackages,
/// inbox rows, group memberships and backups (a dry run only counts them),
/// leaves the other principals' rows, gives the remaining group member a
/// removal hint, and the signed receipt lists every messaging table.
#[tokio::test]
#[ignore = "requires live Postgres; run with cargo test --lib messaging_service::tests::live_backup -- --ignored --test-threads=1"]
async fn live_purge_subject_erases_messaging_rows_and_lists_the_tables_in_the_receipt() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let (client, _shutdown) = serve().await;
    let mut c = client.clone();
    let tenant = uuid::Uuid::new_v4().to_string();
    let group_id = "purge-room";

    let alice = Device::new(&tenant, "alice", "alice-phone");
    let bob = Device::new(&tenant, "bob", "bob-phone");
    for (subject, device) in [("alice", &alice), ("bob", &bob)] {
        register(&mut c, &tenant, subject, device).await;
        publish(&mut c, &tenant, device, 3, true).await;
    }
    let (mut alice_group, info, tree) = alice.create_group(group_id);
    c.create_group(req(
        &tenant,
        msg_pb::CreateGroupRequest {
            tenant_id: tenant.clone(),
            group_id: group_id.to_string(),
            creator_device_id: "alice-phone".to_string(),
            group_info: info,
            ratchet_tree: tree,
        },
    ))
    .await
    .expect("create group");
    let bob_kp = claim(&mut c, &tenant, "alice-phone", "bob-phone").await;
    let (commit, welcome) = alice.add(&mut alice_group, &[bob_kp.key_package]);
    submit(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        0,
        commit,
        welcome,
        "add-bob",
    )
    .await
    .expect("add bob");
    alice.merge(&mut alice_group);
    let bob_before = drain(&mut c, &tenant, "bob-phone", 0).await;
    for subject in ["alice", "bob"] {
        put(&mut c, &tenant, subject, envelope(MIN_BACKUP_BYTES + 64), 0)
            .await
            .expect("store backup");
    }

    let count = |sql: &'static str, value: &'static str| {
        let pool = pool.clone();
        let tenant = tenant.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sql)
                .bind(&tenant)
                .bind(value)
                .fetch_one(&pool)
                .await
                .expect("count messaging rows")
        }
    };
    const DEVICES: &str =
        "SELECT COUNT(*) FROM udb_messaging.devices WHERE tenant_id = $1 AND subject_id = $2";
    const PACKAGES: &str =
        "SELECT COUNT(*) FROM udb_messaging.key_packages WHERE tenant_id = $1 AND device_id = $2";
    const INBOX: &str =
        "SELECT COUNT(*) FROM udb_messaging.inbox WHERE tenant_id = $1 AND device_id = $2";
    const MEMBERS: &str =
        "SELECT COUNT(*) FROM udb_messaging.group_members WHERE tenant_id = $1 AND device_id = $2";
    const BACKUPS: &str =
        "SELECT COUNT(*) FROM udb_messaging.backups WHERE tenant_id = $1 AND subject_id = $2";
    let alice_rows = || async {
        [
            count(DEVICES, "alice").await,
            count(PACKAGES, "alice-phone").await,
            count(INBOX, "alice-phone").await,
            count(MEMBERS, "alice-phone").await,
            count(BACKUPS, "alice").await,
        ]
    };
    let before = alice_rows().await;
    assert!(
        before.iter().all(|rows| *rows > 0),
        "alice has rows in every messaging table: {before:?}"
    );

    let broker = purge_capable_broker_service().await;
    let purge = |dry_run: bool| {
        let mut request = Request::new(PurgeSubjectRequest {
            subject_id: "alice".into(),
            confirm_subject_id: "alice".into(),
            dry_run,
            reason: "messaging erasure test".into(),
            ..Default::default()
        });
        let md = request.metadata_mut();
        md.insert("x-tenant-id", tenant.parse().expect("tenant header"));
        md.insert("x-purpose", "admin".parse().expect("purpose header"));
        md.insert(
            "x-scopes",
            "udb:admin,udb:read,udb:write"
                .parse()
                .expect("scopes header"),
        );
        request
    };
    let messaging_tables = [
        (
            "udb.core.messaging.entity.v1.MessagingInboxEntry",
            "device_id",
        ),
        (
            "udb.core.messaging.entity.v1.MessagingKeyPackage",
            "device_id",
        ),
        (
            "udb.core.messaging.entity.v1.MessagingGroupMember",
            "device_id",
        ),
        ("udb.core.messaging.entity.v1.MessagingDevice", "subject_id"),
        ("udb.core.messaging.entity.v1.MessagingBackup", "subject_id"),
    ];

    let dry = broker
        .purge_subject(purge(true))
        .await
        .expect("dry run")
        .into_inner();
    assert!(dry.dry_run);
    assert_eq!(alice_rows().await, before, "a dry run deletes nothing");
    for (index, (message_type, column)) in messaging_tables.iter().enumerate() {
        let row = dry
            .tables
            .iter()
            .find(|t| t.message_type == *message_type)
            .unwrap_or_else(|| panic!("dry run lists {message_type}"));
        assert_eq!(row.column, *column);
        assert_eq!(
            row.deleted,
            before[[2, 1, 3, 0, 4][index]],
            "{message_type}"
        );
    }

    let erased = broker
        .purge_subject(purge(false))
        .await
        .expect("purge subject")
        .into_inner();
    assert_eq!(
        alice_rows().await,
        [0; 5],
        "every messaging row of alice is gone"
    );
    let expected_total: i64 = before.iter().sum();
    assert_eq!(erased.total_deleted, expected_total);
    // Bob keeps his device, packages, backup and membership row, and his inbox
    // gains a removal hint for the group naming alice's device.
    assert_eq!(count(DEVICES, "bob").await, 1);
    assert_eq!(count(BACKUPS, "bob").await, 1);
    assert_eq!(count(MEMBERS, "bob-phone").await, 1);
    let after_seq = bob_before.last().map(|e| e.seq).unwrap_or_default();
    let hints = drain(&mut c, &tenant, "bob-phone", after_seq).await;
    assert_eq!(hints.len(), 1, "{hints:?}");
    assert_eq!(hints[0].kind, "REMOVAL_HINT");
    assert_eq!(hints[0].group_id, group_id);
    let hint: serde_json::Value = serde_json::from_slice(&hints[0].payload).expect("hint json");
    assert_eq!(hint["device_id"], "alice-phone");
    assert_eq!(hint["reason"], "subject erased");
    assert!(
        erased
            .not_covered
            .iter()
            .any(|line| line.starts_with("messaging group state: 1 group(s)")),
        "{:?}",
        erased.not_covered
    );

    let receipt = verify_erasure_receipt(
        &erased.receipt_jws,
        include_str!("../../../testdata/jwt_rs256_public.pem"),
    )
    .expect("signed erasure receipt verifies");
    let listed: Vec<(String, String, i64)> = receipt["tables"]
        .as_array()
        .expect("receipt tables")
        .iter()
        .map(|t| {
            (
                t["message_type"].as_str().unwrap_or_default().to_string(),
                t["column"].as_str().unwrap_or_default().to_string(),
                t["deleted"].as_i64().unwrap_or(-1),
            )
        })
        .collect();
    for (index, (message_type, column)) in messaging_tables.iter().enumerate() {
        assert!(
            listed.contains(&(
                message_type.to_string(),
                column.to_string(),
                before[[2, 1, 3, 0, 4][index]]
            )),
            "receipt lists {message_type}: {listed:?}"
        );
    }
    assert_eq!(receipt["total_deleted"], expected_total);
    assert!(
        !erased.receipt_json.contains("\"alice\""),
        "the receipt excludes the erased identifier"
    );
}
