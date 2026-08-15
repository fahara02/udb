//! Served two-project/two-instance regression for NotificationService routing.
//!
//! Each request carries one verified tenant/project scope over a real tonic
//! connection. The assertions cover typed reads/writes, raw transactional
//! delivery reporting, preferences/stats, and the project-local outbox.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use uuid::Uuid;

use crate::proto::udb::core::common::v1 as common_pb;
use crate::proto::udb::core::notification::entity::v1 as entity_pb;
use crate::proto::udb::core::notification::services::v1 as notification_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_client::NotificationServiceClient;
use crate::runtime::config::{
    BackendInstance, BackendInstanceConfig, BackendInstanceRole, UdbConfig,
};
use crate::runtime::service::DataBrokerService;
use crate::runtime::{DataBrokerRuntime, native_catalog};

use super::NotificationServiceServer;
use super::model::{delivery_attempt_model, log_model, preference_model};

const PROJECT_A: &str = "notification-project-a";
const PROJECT_B: &str = "notification-project-b";
const PROJECT_C: &str = "notification-shared-project-c";
const PROJECT_D: &str = "notification-shared-project-d";
const TENANT: &str = "notification-project-store-tenant";
const EVENT_TYPE: &str = "notification.project.store";
const PREFERENCE_EVENT: &str = "notification.project.preference";

fn live_pg_dsn() -> String {
    std::env::var("UDB_LIVE_NATIVE_PG_DSN")
        .or_else(|_| std::env::var("UDB_LIVE_AUTH_PG_DSN"))
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string())
}

fn live_enabled() -> bool {
    std::env::var("UDB_LIVE_AUTH_TESTS")
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn dsn_for_database(base_dsn: &str, database: &str) -> String {
    let (without_query, query) = base_dsn
        .split_once('?')
        .map_or((base_dsn, None), |(base, query)| (base, Some(query)));
    let slash = without_query
        .rfind('/')
        .expect("live PostgreSQL DSN must contain a database path");
    let mut dsn = format!("{}{database}", &without_query[..=slash]);
    if let Some(query) = query {
        dsn.push('?');
        dsn.push_str(query);
    }
    dsn
}

fn project_instance(name: &str, project_id: &str, dsn: String) -> BackendInstance {
    BackendInstance {
        name: name.to_string(),
        backend: "postgres".to_string(),
        role: BackendInstanceRole::ReadWrite,
        dsn: Some(dsn),
        dsn_env: None,
        enabled: true,
        read_weight: 1,
        write_weight: 1,
        labels: BTreeMap::from([("project_id".to_string(), project_id.to_string())]),
        capabilities: BTreeSet::new(),
    }
}

fn request_context(project_id: &str) -> common_pb::RequestContext {
    common_pb::RequestContext {
        tenant: Some(common_pb::TenantContext {
            tenant_id: TENANT.to_string(),
            project_id: project_id.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn scoped_request<T>(message: T, project_id: &'static str) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static(TENANT));
    request
        .metadata_mut()
        .insert("x-udb-project-id", MetadataValue::from_static(project_id));
    request
}

async fn provision_native_database(dsn: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(dsn)
        .await
        .unwrap_or_else(|err| panic!("connect Notification project database at {dsn}: {err}"));
    for stmt in native_catalog::native_service_catalog_ddl() {
        sqlx::raw_sql(&stmt)
            .execute(&pool)
            .await
            .unwrap_or_else(|err| {
                panic!("Notification project native DDL failed: {err}\nSQL:\n{stmt}")
            });
    }
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("bootstrap project-local UDB system catalog");
    pool.close().await;
}

async fn activate_project_catalogs(service: &DataBrokerService) {
    for project_id in [PROJECT_A, PROJECT_B, PROJECT_C, PROJECT_D] {
        service
            .catalog
            .stage_catalog(
                native_catalog::native_manifest().clone(),
                project_id.to_string(),
                "1.0.0".to_string(),
                "backward".to_string(),
            )
            .await
            .unwrap_or_else(|err| panic!("stage Notification catalog for {project_id}: {err}"));
        service
            .catalog
            .activate_catalog_for(project_id, "1.0.0")
            .await
            .unwrap_or_else(|err| panic!("activate Notification catalog for {project_id}: {err}"));
    }
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with CREATE DATABASE privilege"]
async fn served_notification_pins_all_paths_to_each_project_instance() {
    if !live_enabled() {
        eprintln!(
            "set UDB_LIVE_AUTH_TESTS=1 to run the live Notification project-store regression"
        );
        return;
    }

    let base_dsn = live_pg_dsn();
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&base_dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live PostgreSQL admin database: {err}"));
    let suffix = Uuid::new_v4().simple().to_string();
    let database_a = format!("udb_notification_a_{suffix}");
    let database_b = format!("udb_notification_b_{suffix}");
    let database_shared = format!("udb_notification_shared_{suffix}");
    for database in [&database_a, &database_b, &database_shared] {
        sqlx::query(&format!("CREATE DATABASE {}", quote_ident(database)))
            .execute(&admin_pool)
            .await
            .unwrap_or_else(|err| panic!("create live Notification database {database}: {err}"));
    }
    let dsn_a = dsn_for_database(&base_dsn, &database_a);
    let dsn_b = dsn_for_database(&base_dsn, &database_b);
    let dsn_shared = dsn_for_database(&base_dsn, &database_shared);
    provision_native_database(&dsn_a).await;
    provision_native_database(&dsn_b).await;
    provision_native_database(&dsn_shared).await;

    let config = UdbConfig {
        project_routing_mode: "strict".to_string(),
        backend_instances: BackendInstanceConfig {
            instances: vec![
                project_instance("notification-a", PROJECT_A, dsn_a.clone()),
                project_instance("notification-b", PROJECT_B, dsn_b.clone()),
                project_instance("notification-c", PROJECT_C, dsn_shared.clone()),
                project_instance("notification-d", PROJECT_D, dsn_shared.clone()),
            ],
        },
        ..UdbConfig::default()
    };
    let runtime = DataBrokerRuntime::from_config(config).await;
    let service =
        DataBrokerService::with_runtime(native_catalog::native_manifest().clone(), runtime);
    activate_project_catalogs(&service).await;
    let notification = service.build_notification_service();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind Notification live server");
    let address = listener
        .local_addr()
        .expect("Notification live server address");
    let incoming = TcpListenerStream::new(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(NotificationServiceServer::new(notification))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve live Notification project-store regression");
    });
    let mut client = NotificationServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect generated Notification client");

    let user_id = Uuid::new_v4().to_string();
    let mut log_ids = BTreeMap::new();
    for (project_id, opted_out) in [(PROJECT_A, true), (PROJECT_B, false)] {
        client
            .upsert_template(scoped_request(
                notification_pb::UpsertTemplateRequest {
                    event_type: EVENT_TYPE.to_string(),
                    channel: entity_pb::NotificationChannel::Email as i32,
                    locale: "en".to_string(),
                    subject_template: format!("Subject for {project_id}"),
                    body_template: format!("Body for {project_id}"),
                    is_active: true,
                    context: Some(request_context(project_id)),
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("upsert template for {project_id}: {err}"));
        client
            .set_preference(scoped_request(
                notification_pb::SetPreferenceRequest {
                    user_id: user_id.clone(),
                    tenant_id: TENANT.to_string(),
                    channel: entity_pb::NotificationChannel::Email as i32,
                    event_type: PREFERENCE_EVENT.to_string(),
                    is_opted_out: opted_out,
                    context: Some(request_context(project_id)),
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("set preference for {project_id}: {err}"));
        let sent = client
            .send_notification(scoped_request(
                notification_pb::SendNotificationRequest {
                    event_type: EVENT_TYPE.to_string(),
                    recipient_address: format!("{project_id}@example.test"),
                    tenant_id: TENANT.to_string(),
                    project_id: project_id.to_string(),
                    channels: vec![entity_pb::NotificationChannel::Email as i32],
                    ..Default::default()
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("send notification for {project_id}: {err}"))
            .into_inner();
        assert_eq!(sent.logs.len(), 1);
        assert_eq!(sent.logs[0].project_id, project_id);
        log_ids.insert(project_id, sent.logs[0].log_id.clone());
    }

    let cross_project = client
        .get_notification(scoped_request(
            notification_pb::GetNotificationRequest {
                log_id: log_ids[PROJECT_A].clone(),
            },
            PROJECT_B,
        ))
        .await
        .expect_err("project B must not read project A's notification");
    assert_eq!(cross_project.code(), tonic::Code::NotFound);

    for project_id in [PROJECT_A, PROJECT_B] {
        let log_id = log_ids[project_id].clone();
        let got = client
            .get_notification(scoped_request(
                notification_pb::GetNotificationRequest {
                    log_id: log_id.clone(),
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("get notification for {project_id}: {err}"))
            .into_inner()
            .log
            .expect("stored notification log");
        assert_eq!(got.project_id, project_id);

        client
            .report_delivery(scoped_request(
                notification_pb::ReportDeliveryRequest {
                    tenant_id: TENANT.to_string(),
                    log_id,
                    channel: entity_pb::NotificationChannel::Email as i32,
                    provider: "live-project-fixture".to_string(),
                    status: entity_pb::NotificationStatus::Delivered as i32,
                    provider_message_id: format!("provider-{project_id}"),
                    context: Some(request_context(project_id)),
                    ..Default::default()
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("report delivery for {project_id}: {err}"));

        let stats = client
            .get_delivery_stats(scoped_request(
                notification_pb::GetDeliveryStatsRequest {
                    tenant_id: TENANT.to_string(),
                    ..Default::default()
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("get delivery stats for {project_id}: {err}"))
            .into_inner();
        assert_eq!(stats.total_delivered, 1);
    }

    // Physical routing alone is insufficient when two active projects share one
    // PostgreSQL database. First-class project columns and conflict predicates
    // must let the same tenant/user/event/channel own distinct rows.
    for (project_id, body, opted_out) in [
        (PROJECT_C, "shared project C body", true),
        (PROJECT_D, "shared project D body", false),
    ] {
        client
            .upsert_template(scoped_request(
                notification_pb::UpsertTemplateRequest {
                    event_type: EVENT_TYPE.to_string(),
                    channel: entity_pb::NotificationChannel::Email as i32,
                    locale: "en".to_string(),
                    subject_template: "Shared authority".to_string(),
                    body_template: body.to_string(),
                    is_active: true,
                    context: Some(request_context(project_id)),
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("upsert shared template for {project_id}: {err}"));
        client
            .set_preference(scoped_request(
                notification_pb::SetPreferenceRequest {
                    user_id: user_id.clone(),
                    tenant_id: TENANT.to_string(),
                    channel: entity_pb::NotificationChannel::Email as i32,
                    event_type: PREFERENCE_EVENT.to_string(),
                    is_opted_out: opted_out,
                    context: Some(request_context(project_id)),
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("set shared preference for {project_id}: {err}"));

        let template = client
            .get_template(scoped_request(
                notification_pb::GetTemplateRequest {
                    event_type: EVENT_TYPE.to_string(),
                    channel: entity_pb::NotificationChannel::Email as i32,
                    locale: "en".to_string(),
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("get shared template for {project_id}: {err}"))
            .into_inner()
            .template
            .expect("shared-project template");
        assert_eq!(template.body_template, body);
        assert_eq!(template.project_id, project_id);

        let preference = client
            .get_preference(scoped_request(
                notification_pb::GetPreferenceRequest {
                    user_id: user_id.clone(),
                    tenant_id: TENANT.to_string(),
                    channel: entity_pb::NotificationChannel::Email as i32,
                    event_type: PREFERENCE_EVENT.to_string(),
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("get shared preference for {project_id}: {err}"))
            .into_inner()
            .preference
            .expect("shared-project preference");
        assert_eq!(preference.is_opted_out, opted_out);
        assert_eq!(preference.project_id, project_id);

        let sent = client
            .send_notification(scoped_request(
                notification_pb::SendNotificationRequest {
                    event_type: EVENT_TYPE.to_string(),
                    recipient_address: format!("{project_id}@example.test"),
                    tenant_id: TENANT.to_string(),
                    project_id: project_id.to_string(),
                    channels: vec![entity_pb::NotificationChannel::Email as i32],
                    ..Default::default()
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("send shared notification for {project_id}: {err}"))
            .into_inner();
        let log_id = sent.logs[0].log_id.clone();
        client
            .report_delivery(scoped_request(
                notification_pb::ReportDeliveryRequest {
                    tenant_id: TENANT.to_string(),
                    log_id: log_id.clone(),
                    channel: entity_pb::NotificationChannel::Email as i32,
                    provider: "live-shared-fixture".to_string(),
                    status: entity_pb::NotificationStatus::Delivered as i32,
                    context: Some(request_context(project_id)),
                    ..Default::default()
                },
                project_id,
            ))
            .await
            .unwrap_or_else(|err| panic!("report shared delivery for {project_id}: {err}"));
        log_ids.insert(project_id, log_id);
    }

    drop(client);
    let _ = shutdown_tx.send(());
    server.await.expect("join Notification live server");
    drop(service);

    let log = log_model();
    let attempt = delivery_attempt_model();
    let preference = preference_model();
    let template = super::model::template_model();
    for (project_id, dsn, expected_opt_out) in
        [(PROJECT_A, &dsn_a, true), (PROJECT_B, &dsn_b, false)]
    {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(dsn)
            .await
            .unwrap_or_else(|err| panic!("inspect Notification database for {project_id}: {err}"));
        let mut tx = pool.begin().await.expect("begin project inspection");
        let inspect_context = crate::RequestContext {
            tenant_id: TENANT.to_string(),
            project_id: project_id.to_string(),
            ..crate::RequestContext::default()
        };
        crate::runtime::core::set_request_local_settings(&mut tx, &inspect_context)
            .await
            .expect("set project inspection context");
        let stored_projects: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT {project}::TEXT FROM {relation} WHERE {tenant} = $1",
            project = log.q("project_id"),
            relation = log.relation,
            tenant = log.q("tenant_id"),
        ))
        .bind(TENANT)
        .fetch_all(&mut *tx)
        .await
        .expect("inspect project notification logs");
        assert_eq!(stored_projects, vec![project_id.to_string()]);
        let attempt_count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {} WHERE {} = $1",
            attempt.relation,
            attempt.q("tenant_id"),
        ))
        .bind(TENANT)
        .fetch_one(&mut *tx)
        .await
        .expect("inspect project delivery attempts");
        assert_eq!(attempt_count, 1);
        let opted_out: bool = sqlx::query_scalar(&format!(
            "SELECT {} FROM {} WHERE {} = $1::UUID AND {} = $2",
            preference.q("is_opted_out"),
            preference.relation,
            preference.q("user_id"),
            preference.q("event_type"),
        ))
        .bind(&user_id)
        .bind(PREFERENCE_EVENT)
        .fetch_one(&mut *tx)
        .await
        .expect("inspect project notification preference");
        assert_eq!(opted_out, expected_opt_out);
        let delivery_events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM udb_system.outbox_events \
             WHERE topic = 'udb.notification.delivery.delivered.v1' AND partition_key = $1",
        )
        .bind(&log_ids[project_id])
        .fetch_one(&mut *tx)
        .await
        .expect("inspect project delivery outbox");
        assert_eq!(delivery_events, 1);
        tx.commit().await.expect("commit project inspection");
        pool.close().await;
    }

    let shared_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn_shared)
        .await
        .expect("inspect shared Notification project database");
    for project_id in [PROJECT_C, PROJECT_D] {
        let mut tx = shared_pool
            .begin()
            .await
            .expect("begin shared-project inspection");
        let inspect_context = crate::RequestContext {
            tenant_id: TENANT.to_string(),
            project_id: project_id.to_string(),
            ..crate::RequestContext::default()
        };
        crate::runtime::core::set_request_local_settings(&mut tx, &inspect_context)
            .await
            .expect("set shared-project inspection context");

        let template_projects: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT {project}::TEXT FROM {relation} WHERE {tenant} = $1 AND {event} = $2",
            project = template.q("project_id"),
            relation = template.relation,
            tenant = template.q("tenant_id"),
            event = template.q("event_type"),
        ))
        .bind(TENANT)
        .bind(EVENT_TYPE)
        .fetch_all(&mut *tx)
        .await
        .expect("inspect one shared-project template");
        assert_eq!(template_projects, vec![project_id.to_string()]);

        let preference_projects: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT {project}::TEXT FROM {relation} WHERE {tenant} = $1 AND {user_id} = $2::UUID \
             AND {event} = $3",
            project = preference.q("project_id"),
            relation = preference.relation,
            tenant = preference.q("tenant_id"),
            user_id = preference.q("user_id"),
            event = preference.q("event_type"),
        ))
        .bind(TENANT)
        .bind(&user_id)
        .bind(PREFERENCE_EVENT)
        .fetch_all(&mut *tx)
        .await
        .expect("inspect one shared-project preference");
        assert_eq!(preference_projects, vec![project_id.to_string()]);

        for model in [&log, &attempt] {
            let projects: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT {project}::TEXT FROM {relation} WHERE {tenant} = $1",
                project = model.q("project_id"),
                relation = model.relation,
                tenant = model.q("tenant_id"),
            ))
            .bind(TENANT)
            .fetch_all(&mut *tx)
            .await
            .expect("inspect one shared-project delivery row");
            assert_eq!(projects, vec![project_id.to_string()]);
        }
        tx.commit().await.expect("commit shared-project inspection");
    }
    shared_pool.close().await;

    for database in [&database_a, &database_b, &database_shared] {
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_ident(database)
        ))
        .execute(&admin_pool)
        .await
        .unwrap_or_else(|err| panic!("drop live Notification database {database}: {err}"));
    }
    admin_pool.close().await;
}
