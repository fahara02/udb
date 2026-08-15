//! Per-operation outbox audit emission for the native `VaultService`.
//! Compatibility operations still use the shared best-effort `emit` method;
//! irreversible/direct-SQL operations can use the strict transactional helper so
//! state and audit evidence cannot diverge. Payloads never carry plaintext,
//! ciphertext, or key material — only tenant/path/version metadata.

use super::super::native_helpers::{
    NativeEventContext, enqueue_outbox_event_in_tx, enqueue_outbox_event_with_context,
};
use super::VaultServiceImpl;

impl VaultServiceImpl {
    /// Emit a per-operation versioned dot-topic outbox event (best-effort). The
    /// payload NEVER carries plaintext — only tenant/path/version metadata.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn emit(
        &self,
        topic: &str,
        partition_key: &str,
        tenant_id: &str,
        project_id: &str,
        operation: &str,
        target_resource: &str,
        payload: serde_json::Value,
    ) {
        let context = crate::RequestContext {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
            ..crate::RequestContext::default()
        };
        let Ok((_context, pool)) = self.resolve_project_store(context, true, "vault_event") else {
            return;
        };
        enqueue_outbox_event_with_context(
            &pool,
            self.outbox_relation.as_deref(),
            topic,
            partition_key,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                operation: operation.to_string(),
                outcome: "allow".to_string(),
                target_resource: target_resource.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}

/// Strict transactional Vault audit event for irreversible/direct-SQL state
/// changes. The caller owns the transaction so the state and its audit evidence
/// either commit together or both roll back.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn enqueue_vault_event_in_tx<'e, E>(
    executor: E,
    outbox_relation: Option<&str>,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    operation: &str,
    target_resource: &str,
    payload: serde_json::Value,
) -> Result<(), String>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    enqueue_outbox_event_in_tx(
        executor,
        outbox_relation,
        topic,
        partition_key,
        tenant_id,
        project_id,
        payload,
        NativeEventContext {
            operation: operation.to_string(),
            outcome: "allow".to_string(),
            target_resource: target_resource.to_string(),
            ..NativeEventContext::default()
        },
    )
    .await
}
