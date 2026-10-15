//! `PurgeSubject` coverage for messaging: erase one principal's devices,
//! KeyPackages, inbox rows, group memberships and backups inside one tenant
//! transaction, and leave every group the principal was in a removal hint so a
//! remaining member commits the removal.
//!
//! The data-plane purge (`handlers_purge.rs`) calls [`MessagingServiceImpl::erase_subject`]
//! after the project's own tables. A store the messaging service was never
//! migrated on is reported as not covered instead of failing the erasure.

use std::collections::BTreeSet;

use tonic::Status;

use super::MessagingServiceImpl;
use super::config::{
    BACKUP_MSG, DEVICE_MSG, GROUP_MEMBER_MSG, INBOX_MSG, KEY_PACKAGE_MSG, KIND_REMOVAL_HINT,
};
use super::handlers::fan_out;
use super::model::removal_hint_payload;
use super::store::{self, DeviceRow, InboxAppend, begin_tenant_tx, commit};

/// The reason a removal hint gives for an erased principal's devices.
const ERASURE_REASON: &str = "subject erased";

/// Rows erased (or, on a dry run, matched) in one messaging table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ErasedTable {
    pub(crate) message_type: &'static str,
    pub(crate) column: &'static str,
    pub(crate) deleted: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MessagingErasure {
    Covered {
        tables: Vec<ErasedTable>,
        /// Groups the principal's devices were members of; each other member
        /// device received a removal hint.
        hinted_groups: usize,
    },
    /// The messaging store is not reachable or not provisioned; the reason.
    NotCovered(String),
}

impl MessagingServiceImpl {
    /// Erase `subject_id`'s messaging rows in `context.tenant_id` (count them
    /// only, with `dry_run`).
    pub(crate) async fn erase_subject(
        &self,
        context: &crate::RequestContext,
        subject_id: &str,
        dry_run: bool,
    ) -> Result<MessagingErasure, Status> {
        let tenant_id = context.tenant_id.trim().to_string();
        let (context, pool) = match self.store_for_context(context.clone(), true) {
            Ok(store) => store,
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                return Ok(MessagingErasure::NotCovered(format!(
                    "messaging: no messaging store is reachable ({})",
                    status.message()
                )));
            }
            Err(status) => return Err(status),
        };
        let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
        if !store::messaging_tables_exist(&mut tx).await? {
            commit(tx).await?;
            return Ok(MessagingErasure::NotCovered(
                "messaging: the messaging tables are not provisioned in this store".to_string(),
            ));
        }
        let devices = store::subject_device_ids(&mut tx, &tenant_id, subject_id).await?;
        let erased: BTreeSet<&str> = devices.iter().map(String::as_str).collect();

        // (target device, group, hint payload, erased device)
        let mut hints: Vec<(String, String, Vec<u8>, String)> = Vec::new();
        let mut groups = BTreeSet::new();
        for device_id in &devices {
            for (group_id, leaf_index) in
                store::groups_of_device(&mut tx, &tenant_id, device_id).await?
            {
                let payload =
                    removal_hint_payload(&group_id, device_id, leaf_index, ERASURE_REASON);
                for member in store::list_members(&mut tx, &tenant_id, &group_id).await? {
                    if !erased.contains(member.device_id.as_str()) {
                        hints.push((
                            member.device_id,
                            group_id.clone(),
                            payload.clone(),
                            device_id.clone(),
                        ));
                    }
                }
                groups.insert(group_id);
            }
        }

        let mut appended = 0;
        if !dry_run && !hints.is_empty() {
            let targets: Vec<String> = hints
                .iter()
                .map(|(target, _, _, _)| target.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let locked = store::lock_devices(&mut tx, &tenant_id, &targets).await?;
            let entries: Vec<InboxAppend<'_>> = hints
                .iter()
                .filter(|(target, _, _, _)| locked.get(target).is_some_and(DeviceRow::is_active))
                .map(|(target, group_id, payload, erased_device)| InboxAppend {
                    device_id: target,
                    kind: KIND_REMOVAL_HINT,
                    group_id,
                    epoch: 0,
                    payload,
                    sender_device_id: erased_device,
                    client_message_id: "",
                })
                .collect();
            appended = fan_out(self, &mut tx, &tenant_id, &context.project_id, entries).await?;
        }

        let m = store::models();
        let delete = !dry_run;
        let subject = [subject_id.to_string()];
        let mut tables = Vec::with_capacity(5);
        for (model, message_type, column, values) in [
            (&m.inbox, INBOX_MSG, "device_id", devices.as_slice()),
            (
                &m.key_package,
                KEY_PACKAGE_MSG,
                "device_id",
                devices.as_slice(),
            ),
            (&m.member, GROUP_MEMBER_MSG, "device_id", devices.as_slice()),
            (&m.device, DEVICE_MSG, "subject_id", subject.as_slice()),
            (&m.backup, BACKUP_MSG, "subject_id", subject.as_slice()),
        ] {
            let deleted =
                store::erase_rows(&mut tx, model, column, &tenant_id, values, delete).await?;
            tables.push(ErasedTable {
                message_type,
                column,
                deleted,
            });
        }
        commit(tx).await?;
        if appended > 0 {
            self.inbox_wake.notify_waiters();
        }
        Ok(MessagingErasure::Covered {
            tables,
            hinted_groups: groups.len(),
        })
    }
}
