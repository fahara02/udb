//! The per-operation outbox audit emission for the native `VaultService`.
//! Extracted verbatim from the former god file — the best-effort
//! `enqueue_outbox_event_with_context` emit is byte-for-byte identical. `emit`
//! stays an inherent method on `VaultServiceImpl` (it uses `self`), shared by the
//! KV / transit / dynamic RPC handlers (including the V-1 audit calls in
//! encrypt/sign/hmac/verify/list_secrets). The payload NEVER carries plaintext,
//! ciphertext, or key material — only tenant/path/version metadata.

use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
use super::VaultServiceImpl;

impl VaultServiceImpl {
    /// Emit a per-operation versioned dot-topic outbox event (best-effort). The
    /// payload NEVER carries plaintext — only tenant/path/version metadata.
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
