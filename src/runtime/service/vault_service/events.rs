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
    /// Build the audit event as a transaction step, so a call site whose write
    /// goes through the dispatch layer can commit both together via
    /// [`crate::runtime::core::DataBrokerRuntime::native_entity_write_co_commit_for_service`].
    ///
    /// This also settles the instance-pinning concern [`Self::emit`] documents:
    /// the event is written by the same transaction as the mutation, on the same
    /// resolved target, so the two cannot land in different project stores.
    ///
    /// `None` means nothing to co-commit — no outbox relation, or a rejected
    /// envelope (warned and counted here, matching `emit`'s best-effort posture).
    pub(crate) fn emit_transaction_op(
        &self,
        context: &crate::RequestContext,
        topic: &str,
        partition_key: &str,
        operation: &str,
        target_resource: &str,
        payload: serde_json::Value,
    ) -> Option<crate::runtime::core::native_store::NativeEntityTransactionOp> {
        match super::super::native_helpers::native_transaction_outbox_op(
            self.outbox_relation.as_deref(),
            topic,
            partition_key,
            &context.tenant_id,
            &context.project_id,
            payload,
            NativeEventContext {
                operation: operation.to_string(),
                outcome: "allow".to_string(),
                target_resource: target_resource.to_string(),
                ..NativeEventContext::default()
            },
        ) {
            Ok(op) => op,
            Err(reject) => {
                tracing::warn!(
                    topic,
                    error = %reject,
                    "refusing to enqueue non-compliant vault audit event; the write still stands"
                );
                self.metrics.inc_outbox_enqueue_failures_total("native");
                None
            }
        }
    }

    /// Best-effort Vault audit event.
    ///
    /// The remaining callers are NOT oversights, so do not convert them to
    /// [`Self::emit_transaction_op`]:
    ///
    /// - `get_secret` / `list_secrets` audit a READ, and the transit operations
    ///   (`encrypt`, `decrypt`, `batch_*`, `generate_data_key`, `rewrap`, `sign`,
    ///   `verify`, `hmac`) perform no durable database write at all. There is no
    ///   transaction to join — the event IS the only durable artifact, so
    ///   "atomic with the write" is not a thing that exists for them.
    /// - the non-Postgres fallback of a co-committed site, where the outbox table
    ///   and the entity write live in different databases.
    ///
    /// Every Vault call site that DOES have a durable Postgres write now commits
    /// its audit with that write, via `emit_transaction_op` or
    /// [`enqueue_vault_event_in_tx`].
    /// The payload NEVER carries plaintext — only tenant/path/version metadata.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn emit(
        &self,
        context: &crate::RequestContext,
        topic: &str,
        partition_key: &str,
        operation: &str,
        target_resource: &str,
        payload: serde_json::Value,
    ) {
        // The caller resolves and pins one physical Vault authority before any
        // durable read/write. Preserve that exact instance for the audit outbox;
        // re-running weighted selection here could put the mutation and its
        // event in different project stores.
        let Ok((context, pool)) = self.resolve_project_store(context.clone(), true, "vault_event")
        else {
            return;
        };
        enqueue_outbox_event_with_context(
            &pool,
            self.outbox_relation.as_deref(),
            topic,
            partition_key,
            &context.tenant_id,
            &context.project_id,
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
