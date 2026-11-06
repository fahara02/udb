//! The data path a caller depends on, through this SDK: a value beyond 2^53 stays exact, a value the column cannot hold is refused, and a delete removes the row.
//!
//! Gated on `UDB_LIVE_SDK_TESTS=1`. CI's "SDK refresh-token rotation (live)" job runs it.

use std::time::Duration;

use tonic::transport::Endpoint;
use tonic::{Code, Request};
use udb_client::proto::udb::core::authn::services::v1 as authn;
use udb_client::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb_client::proto::udb::entity::v1::{DeleteRequest, SelectRequest, UpsertRequest};
use udb_client::proto::udb::services::v1::data_broker_client::DataBrokerClient;
use udb_client::{Metadata, TokenManager};

const FILE: &str = "udb.core.storage.entity.v1.File";
const BEYOND_DOUBLE: &str = "9007199254740993";

fn required(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} is required when UDB_LIVE_SDK_TESTS=1"))
}

fn endpoint(address: &str) -> String {
    if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

fn request<T>(meta: &Metadata, message: T) -> Request<T> {
    let mut request = Request::new(message);
    meta.apply(&mut request).expect("metadata");
    request
}

fn key_filter(id: &str) -> prost_types::Struct {
    prost_types::Struct {
        fields: [(
            "file_id".to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue(id.to_string())),
            },
        )]
        .into_iter()
        .collect(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_data_contract_holds() {
    if std::env::var("UDB_LIVE_SDK_TESTS").as_deref() != Ok("1") {
        eprintln!("skipped: the live data contract needs UDB_LIVE_SDK_TESTS=1");
        return;
    }
    let auth = Endpoint::from_shared(endpoint(&required("UDB_AUTH_GRPC_TARGET")))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .expect("connect the native listener");
    let mut base = Metadata::new(required("UDB_LIVE_TENANT"))
        .with_project(std::env::var("UDB_LIVE_PROJECT").unwrap_or_else(|_| "default".into()));
    base.purpose = "rust.live.data-contract".into();
    base.correlation_id = "rust-live-data-contract".into();
    let tokens = TokenManager::new(auth.clone(), base.clone());
    let login = tokens
        .login(required("UDB_LIVE_USERNAME"), required("UDB_LIVE_PASSWORD"))
        .await
        .expect("login");
    let principal = AuthnServiceClient::new(auth)
        .authenticate(request(
            &base,
            authn::AuthnRequest {
                bearer_token: login.access_token.clone(),
                tenant_hint: base.tenant_id.clone(),
                project_hint: base.project_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("authenticate")
        .into_inner()
        .principal
        .expect("principal");
    let mut meta = tokens.authenticated_metadata().await.expect("metadata");
    meta.tenant_id = principal.tenant_id;

    let channel = Endpoint::from_shared(endpoint(&required("UDB_GRPC_TARGET")))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect the data listener");
    let mut broker = DataBrokerClient::new(channel);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock")
        .as_nanos();
    let id = format!(
        "{:08x}-0000-4000-8000-{:012x}",
        (nanos >> 48) as u32,
        nanos & 0xffff_ffff_ffff
    );
    let row = |size: &str| -> Vec<u8> {
        format!(
            r#"{{"file_id":"{id}","filename":"a.pdf","object_key":"contract/{id}","status":"PENDING","size_bytes":{size}}}"#
        )
        .into_bytes()
    };

    broker
        .upsert(request(
            &meta,
            UpsertRequest {
                message_type: FILE.to_string(),
                record_json: row(BEYOND_DOUBLE),
                ..Default::default()
            },
        ))
        .await
        .expect("an exact integer is stored");
    let read = |broker: DataBrokerClient<tonic::transport::Channel>, id: String, meta: Metadata| async move {
        let mut broker = broker;
        broker
            .select(request(
                &meta,
                SelectRequest {
                    message_type: FILE.to_string(),
                    filter: Some(key_filter(&id)),
                    limit: 1,
                    ..Default::default()
                },
            ))
            .await
            .expect("a read")
            .into_inner()
    };
    let found = read(broker.clone(), id.clone(), meta.clone()).await;
    assert_eq!(found.records_json.len(), 1);
    let text = String::from_utf8_lossy(&found.records_json[0]);
    assert!(text.contains(BEYOND_DOUBLE), "the integer changed: {text}");

    let refused = broker
        .upsert(request(
            &meta,
            UpsertRequest {
                message_type: FILE.to_string(),
                record_json: row("1.5"),
                ..Default::default()
            },
        ))
        .await
        .expect_err("a fraction in an integer column");
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused}");

    broker
        .delete(request(
            &meta,
            DeleteRequest {
                message_type: FILE.to_string(),
                filter: Some(key_filter(&id)),
                ..Default::default()
            },
        ))
        .await
        .expect("a delete");
    let gone = read(broker.clone(), id.clone(), meta.clone()).await;
    assert!(gone.records_json.is_empty(), "the row is gone");

    // No catalog pin unless the caller asks for one, and a pin is honoured when it is sent.
    assert!(meta.client_catalog_version.is_empty(), "a default pin");
    for _ in 0..3 {
        read(broker.clone(), id.clone(), meta.clone()).await;
    }
    let mut pinned = meta.clone();
    pinned.client_catalog_version = "999.0.0".to_string();
    for _ in 0..3 {
        let refused = broker
            .select(request(
                &pinned,
                SelectRequest {
                    message_type: FILE.to_string(),
                    filter: Some(key_filter(&id)),
                    limit: 1,
                    ..Default::default()
                },
            ))
            .await
            .expect_err("a pin the active catalog cannot serve");
        assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    }
}
