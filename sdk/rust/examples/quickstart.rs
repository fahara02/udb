//! Connect, authenticate, and read — the shortest path that touches every part
//! of the client a real caller uses.
//!
//! ```sh
//! export UDB_ENDPOINT=http://127.0.0.1:50051
//! export UDB_TENANT_ID=tenant-1
//! export UDB_USERNAME=alice UDB_PASSWORD=...       # or export UDB_TOKEN directly
//! cargo run --example quickstart -- myapp.v1.Invoice
//! ```
//!
//! This is compiled by `cargo build --examples`, so it is a real compile check on
//! the public API rather than a snippet that can quietly rot.

use std::env;

use udb_client::proto::udb::entity::v1::SelectRequest;
use udb_client::{Metadata, TokenManager, UdbClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::var("UDB_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".into());
    let tenant = env::var("UDB_TENANT_ID").unwrap_or_else(|_| "default".into());
    let project = env::var("UDB_PROJECT_ID").unwrap_or_else(|_| "default".into());
    let message_type = env::args()
        .nth(1)
        .ok_or("usage: quickstart <message_type>, e.g. myapp.v1.Invoice")?;

    // Identity is fixed for the connection; the broker scopes every read and
    // write by it.
    let base = Metadata::new(&tenant).with_project(&project);

    // Two ways in. A pre-minted token is simplest; username/password goes through
    // the authn plane and keeps itself fresh, including rotating the single-use
    // refresh token.
    let meta = match env::var("UDB_TOKEN") {
        Ok(token) if !token.is_empty() => base.with_bearer_token(token),
        _ => {
            let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())?
                .connect()
                .await?;
            let tokens = TokenManager::new(channel, base);
            tokens
                .login(env::var("UDB_USERNAME")?, env::var("UDB_PASSWORD")?)
                .await?;
            // Carries the current access token, refreshing first if it is stale.
            tokens.authenticated_metadata().await?
        }
    };

    let mut udb = UdbClient::connect(endpoint, meta)
        .await?
        // Audit fields are request-scoped; identity above is not overridable.
        .with_audit("quickstart", "corr-quickstart-1");

    // `select_records` decodes the canonical `records_json`. The raw `select`
    // returns a `RecordSet` whose `rows` field is sent EMPTY for wire
    // compatibility, so counting `set.rows` reports the right number of records
    // and reading `set.rows[i].fields` finds nothing in them.
    let records = udb
        .select_records(SelectRequest {
            message_type: message_type.clone(),
            limit: 10,
            ..Default::default()
        })
        .await?;

    println!("{}: {} record(s)", message_type, records.len());
    // Print a real value, not just a count: a count is the one thing that still
    // looks right when the records come back empty.
    for record in records.iter() {
        let summary = record
            .fields()
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("  {summary}");
    }
    if !records.next_page_token().is_empty() {
        println!(
            "more available; next_page_token = {}",
            records.next_page_token()
        );
    }
    Ok(())
}
