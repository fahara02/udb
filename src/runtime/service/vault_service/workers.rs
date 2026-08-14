//! Leader-elected reconciliation for Vault dynamic database credentials.
//!
//! The worker owns every non-terminal split boundary: STARTING issuance claims,
//! expired ACTIVE leases, REVOKING intents, and FAILED revocations. It resolves
//! the immutable physical target recorded on each lease, terminates sessions,
//! removes the role, verifies absence, and only then commits REVOKED plus the
//! strict transactional outbox evidence.

use sqlx::PgPool;
use uuid::Uuid;

use crate::runtime::DataBrokerRuntime;

use super::config::{
    DB_LEASE_ACTIVE, DB_LEASE_FAILED, DB_LEASE_REVOKING, DB_LEASE_STARTING,
};
use super::dynamic::postgres_role_exists;
use super::lifecycle::{
    activate_lease, finalize_revocation, load_reconciliation_candidates, mark_lease_failed,
    transition_to_revoking,
};

pub async fn run_vault_db_lease_reaper_once(
    runtime: &DataBrokerRuntime,
    discovery_pool: &PgPool,
    project_id: &str,
    outbox_relation: Option<&str>,
    batch: i64,
) -> Result<i64, String> {
    let leases = load_reconciliation_candidates(discovery_pool, batch)
        .await
        .map_err(|status| status.message().to_string())?;
    let mut reconciled = 0i64;
    for mut lease in leases {
        if lease.project_id != project_id {
            let err = format!(
                "discovered lease project '{}' does not match worker project '{}'",
                lease.project_id, project_id
            );
            let _ = mark_lease_failed(discovery_pool, &lease.lease_id, &err).await;
            tracing::error!(lease_id = %lease.lease_id, error = %err,
                "vault DB credential reconciliation refused cross-project row");
            continue;
        }
        let context = crate::RequestContext {
            tenant_id: lease.tenant_id.clone(),
            project_id: lease.project_id.clone(),
            target_backend: "postgres".to_string(),
            target_instance: lease.target_instance.clone(),
            ..crate::RequestContext::default()
        };
        let (physical_pool, resolved_instance) = match runtime
            .native_store_postgres_binding_for_service("vault", true, &context)
        {
            Ok(binding) => binding,
            Err(status) => {
                let err = format!(
                    "immutable database credential target is not routable: {}",
                    status.message()
                );
                let _ = mark_lease_failed(discovery_pool, &lease.lease_id, &err).await;
                tracing::error!(lease_id = %lease.lease_id, error = %err,
                    "vault DB credential reconciliation target resolution failed");
                continue;
            }
        };
        if resolved_instance.unwrap_or_default() != lease.target_instance {
            let err = "resolved database credential target does not match immutable lease target";
            let _ = mark_lease_failed(discovery_pool, &lease.lease_id, err).await;
            tracing::error!(lease_id = %lease.lease_id, target_instance = %lease.target_instance,
                "vault DB credential reconciliation target drift refused");
            continue;
        }

        if lease.state == DB_LEASE_STARTING {
            match postgres_role_exists(&physical_pool, &lease.username).await {
                Ok(true) => match activate_lease(
                    discovery_pool,
                    outbox_relation,
                    &lease,
                    "reconcile_database_credential_issuance",
                )
                .await
                {
                    Ok(true) => reconciled += 1,
                    Ok(false) => {}
                    Err(status) => {
                        tracing::warn!(lease_id = %lease.lease_id, error = %status,
                            "vault DB credential STARTING activation retry failed");
                    }
                },
                Ok(false) => {
                    let err = "STARTING lease has no physical role after reconciliation grace";
                    if mark_lease_failed(discovery_pool, &lease.lease_id, err)
                        .await
                        .is_ok()
                    {
                        reconciled += 1;
                    }
                }
                Err(err) => {
                    tracing::warn!(lease_id = %lease.lease_id, error = %err,
                        "vault DB credential STARTING role proof failed");
                }
            }
            continue;
        }

        let needs_revocation = (lease.state == DB_LEASE_ACTIVE && lease.expires_at <= chrono::Utc::now())
            || lease.state == DB_LEASE_REVOKING
            || (lease.state == DB_LEASE_FAILED && lease.revocation_requested);
        if !needs_revocation {
            continue;
        }
        let operation_id = if lease.revocation_operation_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            lease.revocation_operation_id.clone()
        };
        if lease.state != DB_LEASE_REVOKING {
            if let Err(status) = transition_to_revoking(
                discovery_pool,
                &lease.lease_id,
                &operation_id,
                if lease.state == DB_LEASE_ACTIVE {
                    "lease expired"
                } else {
                    "retry failed revocation"
                },
            )
            .await
            {
                tracing::warn!(lease_id = %lease.lease_id, error = %status,
                    "vault DB credential REVOKING transition failed");
                continue;
            }
            lease.state = DB_LEASE_REVOKING.to_string();
            lease.revocation_operation_id = operation_id.clone();
            lease.revocation_requested = true;
        }
        match finalize_revocation(
            &physical_pool,
            outbox_relation,
            &lease,
            &operation_id,
            "reconcile_database_credential_revocation",
        )
        .await
        {
            Ok(()) => reconciled += 1,
            Err(status) => {
                let _ = mark_lease_failed(discovery_pool, &lease.lease_id, status.message()).await;
                tracing::warn!(lease_id = %lease.lease_id, error = %status,
                    "vault DB credential revocation reconciliation failed; durable intent retained");
            }
        }
    }
    Ok(reconciled)
}
