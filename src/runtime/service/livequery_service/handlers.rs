//! The `Subscribe` RPC handler — tenant-scoped initial snapshot (mediated IR read)
//! followed by the bounded delta stream. Extracted from the trait impl; `mod.rs`
//! delegates one line to it.

use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::ir::{LogicalPagination, LogicalRead};
use crate::proto::udb::core::livequery::services::v1 as lq_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{admit_on as native_admit_on, validated_native_service_context};
use super::LiveQueryServiceImpl;
use super::budget::try_acquire_stream_slot;
use super::config::{
    SERVICE_ID, buffer_events, clamp_snapshot_limit, default_snapshot_limit, max_snapshot_limit,
    resume_replay_limit,
};
use super::errors::livequery_required_field;
use super::predicate::{
    build_user_filter, parse_resume_cursor, resolve_source, row_object, snapshot_filter,
};
use super::stream::{JournalTail, LiveQueryStream, run_delta_forward};

/// The terminal frame of a subscription on a broker with no CDC change feed:
/// the snapshot was delivered, but no delta ever will be.
pub(super) fn no_delta_feed_status() -> Status {
    super::errors::livequery_capability_status(
        "delta_feed",
        "cdc_change_feed",
        "live query deltas require the CDC change feed (UDB_CDC_ENABLED with Kafka); \
         this broker has none, so the stream ends after the snapshot",
    )
}

/// Request-metadata header carrying the durable-resume cursor: the client's
/// last-delivered `LiveQueryChange.event_id`. Metadata (not a proto field) so
/// resume needs no contract change — a fresh subscription simply omits it.
const RESUME_CURSOR_HEADER: &str = "x-udb-livequery-resume";

/// Pick the resume cursor from the typed field and the legacy header. Both may
/// be set only when they name the same event: two different cursors mean the
/// client's resume state is confused, and guessing one would skip or repeat
/// changes.
pub(super) fn resolve_resume_cursor(
    since_event_id: &str,
    header: Option<&str>,
) -> Result<Option<String>, Status> {
    let typed = parse_resume_cursor(Some(since_event_id));
    let legacy = parse_resume_cursor(header);
    match (typed, legacy) {
        (Some(typed), Some(legacy)) if typed != legacy => {
            Err(crate::runtime::executor_utils::invalid_argument_fields(
                format!(
                    "since_event_id '{typed}' and the {RESUME_CURSOR_HEADER} header '{legacy}' name different resume points"
                ),
                [(
                    "since_event_id",
                    "set since_event_id only; drop the legacy resume header",
                )],
            ))
        }
        (Some(typed), _) => Ok(Some(typed)),
        (None, legacy) => Ok(legacy),
    }
}

/// A subscription's predicates read the columns they name, for the snapshot and
/// for every delta matched afterwards: a predicate on a PII, encrypted or
/// masked column answers the question the masked rows hide. It follows the
/// Select filter rule, judged against the subscriber's context (whose rows are
/// masked by the same scope test below).
///
/// `verified_purpose` is the purpose the subscriber's verified bearer token
/// asserts (see [`verified_bearer_purpose`]); only that one may count as a
/// privileged purpose. The context's own `purpose` is never verified.
fn enforce_live_filter_export_controls(
    manifest: &crate::generation::CatalogManifest,
    context: &crate::RequestContext,
    verified_purpose: Option<&str>,
    message_type: &str,
    req: &lq_pb::SubscribeRequest,
) -> Result<(), Status> {
    let fields: Vec<String> = req
        .filters
        .iter()
        .chain(req.any_of.iter().flat_map(|group| group.predicates.iter()))
        .map(|predicate| predicate.field.trim().to_string())
        .filter(|field| !field.is_empty())
        .collect();
    let security = crate::runtime::security::SecurityContext {
        tenant_id: context.tenant_id.clone(),
        project_id: context.project_id.clone(),
        purpose: verified_purpose
            .map(ToString::to_string)
            .unwrap_or_else(|| context.purpose.clone()),
        purpose_verified: verified_purpose.is_some(),
        scopes: context.scopes.clone(),
        ..crate::runtime::security::SecurityContext::default()
    };
    crate::runtime::security::enforce_filter_export_controls(
        manifest,
        &security,
        message_type,
        None,
        &fields,
    )
}

/// The purpose claim of the subscriber's verified bearer token, if it has one.
/// Taken only from the bearer principal the credential layer verified, and
/// only when that principal is the one the method-security gate admitted
/// (same subject and credential id). A purpose in a request header is never
/// returned.
fn verified_bearer_purpose(
    claim: &crate::runtime::service::method_security::VerifiedClaimContext,
    preresolved: Option<&crate::runtime::credential_layer::PreresolvedCredentials>,
) -> Option<String> {
    if !claim.authenticated {
        return None;
    }
    let principal = preresolved?.bearer.as_ref()?.as_ref().ok()?;
    if principal.subject != claim.subject || principal.credential_id != claim.credential_id {
        return None;
    }
    principal
        .purpose
        .clone()
        .filter(|purpose| !purpose.trim().is_empty())
}

/// Give the native context the subscriber's verified scopes. The native
/// context is built from request metadata and carries none, so a subscriber
/// holding `udb:pii:read` was refused a predicate on a PII column and saw
/// masked rows it may read. Only an authenticated claim from the
/// method-security gate supplies scopes; an in-process call without one keeps
/// the context as built.
fn with_verified_scopes(
    mut context: crate::RequestContext,
    claim: &crate::runtime::service::method_security::VerifiedClaimContext,
) -> crate::RequestContext {
    if claim.authenticated {
        context.scopes = claim.scopes.clone();
    }
    context
}

pub(crate) async fn subscribe(
    svc: &LiveQueryServiceImpl,
    request: Request<lq_pb::SubscribeRequest>,
) -> Result<Response<LiveQueryStream>, Status> {
    let metadata = request.metadata().clone();
    let claim = crate::runtime::service::method_security::current_claim_context();
    let verified_purpose = verified_bearer_purpose(
        &claim,
        request
            .extensions()
            .get::<crate::runtime::credential_layer::PreresolvedCredentials>(),
    );
    let req = request.into_inner();
    // Scope guard FIRST: body tenant/project must match the verified claim and
    // request metadata before either value reaches the runtime context.
    let tenant_id = req.tenant_id.trim().to_string();
    let message_type = req.message_type.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let context = with_verified_scopes(
        validated_native_service_context(&metadata, &tenant_id, &project_id)?,
        &claim,
    );
    if message_type.is_empty() {
        return Err(livequery_required_field(
            "message_type",
            "must be a non-empty source message type",
            "message_type is required",
        ));
    }
    // A native entity, else a table of the project's active catalog. Fail closed BEFORE
    // any read/stream if the source is unknown or has no tenant column.
    let native = crate::runtime::native_catalog::native_manifest();
    let project_catalog = if crate::broker::resolve_table_for_message(native, &message_type).is_ok()
    {
        None
    } else {
        svc.require_runtime()?
            .load_active_catalog_for_project(&project_id)
            .await?
    };
    let source_manifest = project_catalog
        .as_ref()
        .map_or(native, |catalog| &catalog.manifest);
    let source = resolve_source(&message_type, source_manifest)?;
    let user_filter = build_user_filter(&req.filters, &req.any_of)?;
    enforce_live_filter_export_controls(
        source_manifest,
        &context,
        verified_purpose.as_deref(),
        &message_type,
        &req,
    )?;

    // Durable-resume cursor (optional): the client's last-delivered event_id.
    // The typed `since_event_id` field is canonical; the older
    // `x-udb-livequery-resume` header is still honoured. Absent/blank = a fresh,
    // non-resuming subscription.
    let resume_cursor = resolve_resume_cursor(
        &req.since_event_id,
        metadata
            .get(RESUME_CURSOR_HEADER)
            .and_then(|value| value.to_str().ok()),
    )?;

    // Rate-bound new subscriptions per tenant at entry; the permit is dropped
    // immediately (not held across the long-lived stream) so it cannot block.
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        SERVICE_ID,
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;

    // Concurrency budget (distinct from the rate bound above): cap the
    // tenant's ACTIVE long-lived streams before any read/stream resources
    // are committed. The slot is released by the guard's Drop when the
    // stream ends, on every path.
    let stream_slot = try_acquire_stream_slot(&tenant_id)?;

    let runtime = svc.require_runtime()?;

    // Subscribe to the delta feed BEFORE the snapshot so events committed
    // during the snapshot are buffered on the broadcast receiver (not lost in
    // the subscribe→read gap); any overlap with snapshot rows is de-duplicated
    // client-side by event_id.
    let delta_rx = svc.cdc_engine.as_ref().map(|cdc| cdc.subscribe());
    // Cross-replica delta source: the broadcast above is fed only on the
    // replica holding the CDC tailer lease, so anchor a durable-journal tail at
    // the journal head NOW (also before the snapshot) — every replica then
    // streams the deltas committed after this point. A journal that cannot be
    // read REFUSES the subscription (Unavailable, retryable): falling back to
    // the broadcast alone would silently deliver nothing on every replica but
    // the tailer's leader.
    // The journal itself is scanned by one shared poller per (topic, tenant,
    // project); join it BEFORE reading this subscriber's head so no batch
    // produced after the head can be missed.
    let journal_head = match svc.cdc_engine.as_ref() {
        Some(cdc) => match async {
            let feed = super::shared_tail::join(
                cdc,
                &source.cdc_topic,
                &tenant_id,
                &project_id,
                i64::from(resume_replay_limit()),
                svc.metrics.clone(),
            )
            .await?;
            cdc.journal_head_watermark(&source.cdc_topic)
                .await
                .map(|head| (head, feed))
        }
        .await
        {
            Ok((head, feed)) => Some((cdc.clone(), head, feed)),
            Err(err) => {
                tracing::warn!(
                    topic = %source.cdc_topic,
                    error = %err,
                    "live query subscription refused: the CDC journal head cannot be read"
                );
                return Err(super::errors::livequery_journal_unavailable_status(
                    "journal_head",
                ));
            }
        },
        None => None,
    };

    // Durable resume: when the client presented a resume cursor AND a live CDC
    // feed exists, read the missed change events from the durable CDC journal
    // (tenant re-checked at read time with the canonical scope predicate) so they
    // can be replayed to the client BEFORE the broadcast hand-off — closing the
    // reconnect gap the in-process broadcast ring cannot span. Bounded by
    // `resume_replay_limit()`; a larger gap is closed by a subsequent re-resume
    // from the client's new last-delivered cursor.
    // An unreadable journal refuses the resume rather than replaying an empty
    // backlog the client would take as "nothing was missed".
    let (resume_replay, resume_position) = match (resume_cursor.as_deref(), svc.cdc_engine.as_ref())
    {
        (Some(cursor), Some(cdc)) => cdc
            .try_positioned_replay_for_scope(
                &source.cdc_topic,
                &tenant_id,
                &project_id,
                cursor,
                i64::from(resume_replay_limit()),
            )
            .await
            .map_err(|err| {
                tracing::warn!(
                    topic = %source.cdc_topic,
                    error = %err,
                    "live query resume refused: the CDC journal cannot be read"
                );
                err
            })
            .map(|(events, position)| (events, Some(position)))?,
        _ => (Vec::new(), None),
    };

    let limit = clamp_snapshot_limit(
        req.snapshot_limit,
        default_snapshot_limit(),
        max_snapshot_limit(),
    );
    let read = LogicalRead {
        message_type: message_type.clone(),
        // A shared table has no tenant column to scope on: every tenant reads
        // all of its rows, so only the caller's predicates apply.
        filter: if source.shared {
            user_filter.clone()
        } else {
            Some(snapshot_filter(
                &source.tenant_field,
                &tenant_id,
                user_filter.clone(),
            ))
        },
        projection: None,
        sort: Vec::new(),
        include: Vec::new(),
        // One past the limit, so a cut snapshot can say so.
        pagination: Some(LogicalPagination::limit(limit.saturating_add(1))),
    };
    // MEDIATED snapshot: IR query plan + read-fence (carried on the context),
    // tenant predicate injected server-side. Never a raw query. A catalog table reads
    // through Select, so masking, soft delete and RLS apply as on any other read.
    let mut matched_rows = None;
    let rows = if project_catalog.is_some() {
        let filter = user_filter
            .as_ref()
            .map(super::predicate::logical_filter_to_json)
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        // The stream's own authorization admitted this read; the data plane asks for it
        // in its own terms.
        let mut read_context = context.clone();
        if read_context.purpose.trim().is_empty() {
            read_context.purpose = "live_query".to_string();
        }
        if !read_context.scopes.iter().any(|scope| scope == "udb:read") {
            read_context.scopes.push("udb:read".to_string());
        }
        let (records, _warning) = runtime
            .select(
                source_manifest,
                crate::proto::SelectRequest {
                    message_type: message_type.clone(),
                    filter: crate::runtime::executor_utils::json_to_struct(&filter),
                    limit: i32::try_from(limit).unwrap_or(i32::MAX),
                    include_total: true,
                    ..Default::default()
                },
                read_context,
            )
            .await?;
        matched_rows = Some(records.exact_total);
        records
            .records_json
            .iter()
            .filter_map(|raw| serde_json::from_slice(raw).ok())
            .collect::<Vec<serde_json::Value>>()
    } else {
        let mut rows = runtime
            .native_entity_read_for_service(SERVICE_ID, &context, read)
            .await?;
        let keep = usize::try_from(limit).unwrap_or(usize::MAX);
        if rows.len() > keep {
            matched_rows = Some(rows.len() as i64);
            rows.truncate(keep);
        }
        rows
    };
    // PII masking for BOTH the snapshot and the live deltas. The relational read
    // path masks these columns; LiveQuery shipped the raw row image, so a
    // subscriber with only `udb:stream` saw them in clear text.
    let can_read_pii = context
        .scopes
        .iter()
        .any(|scope| scope == "udb:pii:read" || scope == "udb:*" || scope == "*");
    let masked_columns: Vec<String> = if can_read_pii {
        Vec::new()
    } else {
        source_manifest
            .tables
            .iter()
            .find(|table| {
                crate::runtime::projection::message_type_matches(
                    &table.message_fqn(),
                    &message_type,
                )
            })
            .map(|table| {
                table
                    .columns
                    .iter()
                    .filter(|c| {
                        c.security.is_pii
                            || c.security.mask_in_logs
                            || crate::runtime::executor_utils::is_encrypted_column(c)
                    })
                    .map(|c| c.column_name.clone())
                    .collect()
            })
            .unwrap_or_default()
    };
    let rows_json: Vec<String> = rows
        .iter()
        .map(|row| {
            let masked = super::predicate::mask_row(row_object(row), &masked_columns);
            serde_json::to_string(&masked).unwrap_or_else(|_| "{}".to_string())
        })
        .collect();
    let snapshot = lq_pb::SubscribeResponse {
        payload: Some(lq_pb::subscribe_response::Payload::Snapshot(
            lq_pb::LiveQuerySnapshot {
                // Every row that matched: above rows_json's length, the snapshot was
                // cut at its limit.
                row_count: matched_rows.unwrap_or(rows_json.len() as i64),
                rows_json,
            },
        )),
        error: None,
    };

    let (tx, mut rx) = mpsc::channel::<Result<lq_pb::SubscribeResponse, Status>>(buffer_events());
    // The snapshot is the FIRST frame (the channel is empty, so this fits).
    let _ = tx.try_send(Ok(snapshot));

    // Spawn the bounded delta forwarder when a live CDC feed exists; otherwise
    // drop the sender so the stream ends cleanly after the snapshot. The
    // budget slot travels with the forwarder task (released when the task
    // exits); without a live feed the stream is snapshot-only and ends
    // immediately, so the slot is released right here.
    if let Some(rx_delta) = delta_rx {
        let journal_tail = journal_head.map(|(cdc, watermark, feed)| JournalTail {
            cdc,
            watermark: resume_position.unwrap_or(watermark),
            batch: i64::from(resume_replay_limit()),
            feed,
        });
        tokio::spawn(run_delta_forward(
            rx_delta,
            tx,
            tenant_id,
            project_id,
            source.cdc_topic,
            masked_columns,
            user_filter,
            resume_replay,
            svc.metrics.clone(),
            stream_slot,
            journal_tail,
        ));
    } else {
        // No change feed exists on this broker, so no delta can ever arrive.
        // Say so on the stream (after the snapshot) instead of ending it
        // cleanly, which a client cannot tell apart from "nothing changed".
        let _ = tx.try_send(Err(no_delta_feed_status()));
        drop(tx);
        drop(stream_slot);
    }

    let stream = async_stream::try_stream! {
        while let Some(item) = rx.recv().await {
            let frame = item?;
            yield frame;
        }
    };
    Ok(Response::new(Box::pin(stream)))
}

#[cfg(test)]
mod filter_export_control_tests {
    use super::*;
    use crate::generation::{
        CatalogManifest, ManifestColumn, ManifestColumnSecurity, ManifestTable,
    };

    fn manifest() -> CatalogManifest {
        CatalogManifest {
            tables: vec![ManifestTable {
                message_name: "Customer".to_string(),
                columns: vec![
                    ManifestColumn {
                        field_name: "status".to_string(),
                        column_name: "status".to_string(),
                        ..ManifestColumn::default()
                    },
                    ManifestColumn {
                        field_name: "phone".to_string(),
                        column_name: "phone".to_string(),
                        security: ManifestColumnSecurity {
                            mask_in_logs: true,
                            ..ManifestColumnSecurity::default()
                        },
                        ..ManifestColumn::default()
                    },
                ],
                ..ManifestTable::default()
            }],
            ..CatalogManifest::default()
        }
    }

    fn on(field: &str) -> lq_pb::LiveQueryPredicate {
        lq_pb::LiveQueryPredicate {
            field: field.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn a_live_query_predicate_on_a_masked_column_needs_the_pii_scope() {
        let subscriber = crate::RequestContext {
            tenant_id: "acme".to_string(),
            ..crate::RequestContext::default()
        };
        let plain = lq_pb::SubscribeRequest {
            filters: vec![on("status")],
            ..Default::default()
        };
        enforce_live_filter_export_controls(&manifest(), &subscriber, None, "Customer", &plain)
            .expect("a predicate on an unmasked column");
        let probe = lq_pb::SubscribeRequest {
            any_of: vec![lq_pb::LiveQueryAnyOf {
                predicates: vec![on("status"), on("PHONE")],
            }],
            ..Default::default()
        };
        let err =
            enforce_live_filter_export_controls(&manifest(), &subscriber, None, "Customer", &probe)
                .expect_err("a predicate on a masked column needs the scope");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        let scoped = crate::RequestContext {
            scopes: vec!["udb:pii:read".to_string()],
            ..subscriber
        };
        enforce_live_filter_export_controls(&manifest(), &scoped, None, "Customer", &probe)
            .expect("udb:pii:read may filter on it");

        // A purpose the verified token asserts counts; the same purpose stated
        // only on the context (a header) does not.
        let header_purpose = crate::RequestContext {
            purpose: "audit".to_string(),
            ..crate::RequestContext::default()
        };
        enforce_live_filter_export_controls(&manifest(), &header_purpose, None, "Customer", &probe)
            .expect_err("a header purpose unlocks nothing");
        enforce_live_filter_export_controls(
            &manifest(),
            &header_purpose,
            Some("audit"),
            "Customer",
            &probe,
        )
        .expect("a verified audit purpose may filter on a masked column");
    }

    fn claim(scopes: &[&str]) -> crate::runtime::service::method_security::VerifiedClaimContext {
        crate::runtime::service::method_security::VerifiedClaimContext {
            subject: "user-a".to_string(),
            tenant_id: "acme".to_string(),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            credential_id: "jwt-a".to_string(),
            authenticated: true,
            ..Default::default()
        }
    }

    // The native context carries no scopes; the subscriber's verified scopes
    // must reach the filter check and the mask.
    #[test]
    fn a_subscriber_holding_the_pii_scope_may_filter_on_a_masked_column() {
        let native = crate::RequestContext {
            tenant_id: "acme".to_string(),
            ..crate::RequestContext::default()
        };
        let probe = lq_pb::SubscribeRequest {
            filters: vec![on("phone")],
            ..Default::default()
        };
        let context = with_verified_scopes(native.clone(), &claim(&["udb:stream", "udb:pii:read"]));
        assert_eq!(context.scopes, vec!["udb:stream", "udb:pii:read"]);
        enforce_live_filter_export_controls(&manifest(), &context, None, "Customer", &probe)
            .expect("the verified udb:pii:read scope is honoured");

        // No authenticated claim (in-process call): nothing is added.
        let unauthenticated = crate::runtime::service::method_security::VerifiedClaimContext {
            scopes: vec!["udb:pii:read".to_string()],
            ..Default::default()
        };
        assert!(
            with_verified_scopes(native, &unauthenticated)
                .scopes
                .is_empty()
        );
    }

    #[test]
    fn only_the_admitted_bearer_supplies_a_verified_purpose() {
        let principal = crate::runtime::credential_layer::VerifiedPrincipal {
            subject: "user-a".to_string(),
            credential_id: "jwt-a".to_string(),
            purpose: Some("export".to_string()),
            ..Default::default()
        };
        let credentials = crate::runtime::credential_layer::PreresolvedCredentials {
            bearer: Some(Ok(principal.clone())),
            ..Default::default()
        };
        assert_eq!(
            verified_bearer_purpose(&claim(&[]), Some(&credentials)).as_deref(),
            Some("export")
        );
        assert_eq!(verified_bearer_purpose(&claim(&[]), None), None);
        let unauthenticated = crate::runtime::service::method_security::VerifiedClaimContext {
            authenticated: false,
            ..claim(&[])
        };
        assert_eq!(
            verified_bearer_purpose(&unauthenticated, Some(&credentials)),
            None
        );
        // A bearer that is not the admitted principal is ignored.
        let other = crate::runtime::credential_layer::PreresolvedCredentials {
            bearer: Some(Ok(crate::runtime::credential_layer::VerifiedPrincipal {
                credential_id: "jwt-b".to_string(),
                ..principal
            })),
            ..Default::default()
        };
        assert_eq!(verified_bearer_purpose(&claim(&[]), Some(&other)), None);
    }
}
