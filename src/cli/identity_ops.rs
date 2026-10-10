//! Declarative identity reconciliation through the canonical native services.
//! Account/grant/key responses remain authoritative. Local output receipts only
//! prevent repeating a one-time secret operation; a pending outcome is refused.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};
use udb::proto::udb::core::apikey::entity::v1 as key_entity;
use udb::proto::udb::core::apikey::services::v1 as key_pb;
use udb::proto::udb::core::apikey::services::v1::api_key_service_client::ApiKeyServiceClient;
use udb::proto::udb::core::authn::entity::v1 as user_pb;
use udb::proto::udb::core::authn::services::v1 as authn_pb;
use udb::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb::proto::udb::core::common::v1 as common;

const MAX_ACCOUNTS: usize = 128;
const MAX_KEYS: usize = 32;
const MAX_PAGES: usize = 1024;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SecretRef {
    #[serde(default)]
    env: String,
    #[serde(default)]
    file: String,
}

// No Debug/Serialize implementation: a resolved credential never enters a report.
struct Secret(String);

impl SecretRef {
    fn validate(&self) -> Result<(), String> {
        if self.env.is_empty() == self.file.is_empty()
            || (!self.env.is_empty()
                && (!self
                    .env
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    || self.env.as_bytes()[0].is_ascii_digit()))
        {
            return Err("password_ref requires exactly one valid env or file reference".into());
        }
        Ok(())
    }

    fn resolve(&self, base: &Path) -> Result<Secret, String> {
        self.validate()?;
        let raw = if !self.env.is_empty() {
            std::env::var(&self.env).map_err(|_| "password environment reference is unavailable")?
        } else {
            read_private(&base.join(&self.file), 4096)?
        };
        let raw = raw.trim_end_matches(['\r', '\n']).to_string();
        if raw.is_empty() || raw.len() > 4096 || raw.chars().any(char::is_control) {
            return Err("password reference must contain one nonempty bounded line".into());
        }
        udb::runtime::authn::PasswordPolicy::from_env()
            .validate(&raw)
            .map_err(|_| "password reference does not satisfy the local password policy")?;
        Ok(Secret(raw))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProvisionAccount {
    username: String,
    email: String,
    password_ref: SecretRef,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeclaredKey {
    name: String,
    scopes: Vec<String>,
    secret_output: String,
    // Explicit old prefix, never a boolean that would rotate on every apply.
    #[serde(default)]
    rotate_from: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeclaredAccount {
    #[serde(default)]
    pub(super) account: String,
    pub(super) identity: String,
    #[serde(default)]
    pub(super) project: String,
    pub(super) scopes: Vec<String>,
    #[serde(default)]
    pub(super) reason: String,
    #[serde(default)]
    provision: Option<ProvisionAccount>,
    #[serde(default)]
    api_keys: Vec<DeclaredKey>,
}

impl DeclaredAccount {
    /// The `up` bridge serializes several declarations into one temporary file.
    /// Preserve each declaration's original reference base before that move.
    pub(super) fn resolve_file_references(&mut self, base: &Path) -> Result<(), String> {
        let absolute = |value: &str| {
            base.join(value)
                .to_str()
                .map(str::to_string)
                .ok_or_else(|| "identity reference path is not valid UTF-8".to_string())
        };
        if let Some(provision) = &mut self.provision {
            if !provision.password_ref.file.is_empty() {
                provision.password_ref.file = absolute(&provision.password_ref.file)?;
            }
        }
        for key in &mut self.api_keys {
            key.secret_output = absolute(&key.secret_output)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IdentityFile {
    #[serde(default)]
    pub(super) tenant: String,
    pub(super) service_accounts: Vec<DeclaredAccount>,
}

#[derive(Debug, Clone, Serialize)]
struct LiveGrant {
    account: String,
    identity: String,
    project: String,
    scopes: Vec<String>,
    active: bool,
    revision: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum IdentityStep {
    Unchanged,
    Create,
    Transfer {
        from: String,
        expected_revision: i64,
    },
    RotateIdentity {
        expected_revision: i64,
    },
    ReplaceScopes {
        expected_revision: i64,
    },
    Refused {
        reason: String,
        detail: String,
    },
}

fn sorted_scopes(scopes: &[String]) -> Vec<String> {
    let mut out: Vec<_> = scopes
        .iter()
        .map(|s| s.trim().to_ascii_lowercase())
        .collect();
    out.sort();
    out.dedup();
    out
}

fn checked_scopes(requested: &[String], approved: &[String]) -> Result<Vec<String>, String> {
    if approved.is_empty()
        || approved
            .iter()
            .chain(requested)
            .any(|s| s.trim().is_empty())
    {
        return Err("scope lists must contain nonempty entries".into());
    }
    udb::runtime::service::validate_declared_service_scopes(requested, approved)
        .map_err(|_| "service scopes are forbidden or outside the declared grant".into())
}

fn plan_identity(
    d: &DeclaredAccount,
    live: &[LiveGrant],
    allow_transfer: bool,
) -> Vec<IdentityStep> {
    let own = live.iter().find(|g| g.account == d.account);
    let holder = live
        .iter()
        .find(|g| g.identity == d.identity && g.account != d.account);
    let differ = |g: &LiveGrant| {
        sorted_scopes(&g.scopes) != sorted_scopes(&d.scopes) || g.project != d.project
    };
    match (own, holder) {
        (Some(g), _) if !g.active => vec![IdentityStep::Refused {
            reason: "grant_revoked".into(),
            detail: "a revoked grant is final; use a new service account".into(),
        }],
        (Some(_), Some(_)) => vec![IdentityStep::Refused {
            reason: "UDB_GRANT_OWNED_BY_OTHER".into(),
            detail: "the account and identity already hold different grants".into(),
        }],
        (Some(g), None) => {
            let mut steps = vec![];
            if g.identity != d.identity {
                steps.push(IdentityStep::RotateIdentity {
                    expected_revision: g.revision,
                });
            }
            if differ(g) {
                steps.push(IdentityStep::ReplaceScopes {
                    expected_revision: g.revision,
                });
            }
            if steps.is_empty() {
                steps.push(IdentityStep::Unchanged);
            }
            steps
        }
        (None, Some(_)) if !allow_transfer => vec![IdentityStep::Refused {
            reason: "UDB_GRANT_OWNED_BY_OTHER".into(),
            detail: "an explicit --allow-transfer is required".into(),
        }],
        (None, Some(g)) => {
            let mut steps = vec![IdentityStep::Transfer {
                from: g.account.clone(),
                expected_revision: g.revision,
            }];
            if differ(g) {
                steps.push(IdentityStep::ReplaceScopes {
                    expected_revision: g.revision + 1,
                });
            }
            steps
        }
        (None, None) => vec![IdentityStep::Create],
    }
}

fn bounded_field(s: &str, max: usize, name: &str, empty: bool) -> Result<(), String> {
    if (!empty && s.is_empty()) || s != s.trim() || s.len() > max || s.chars().any(char::is_control)
    {
        Err(format!("invalid {name}"))
    } else {
        Ok(())
    }
}

fn canonical_uuid(s: &str) -> Result<(), String> {
    if uuid::Uuid::parse_str(s)
        .map(|id| id.to_string())
        .ok()
        .as_deref()
        != Some(s)
    {
        return Err("account must be a canonical server-assigned UUID".into());
    }
    Ok(())
}

fn validate_declarations(file: &IdentityFile, tenant: &str, base: &Path) -> Result<(), String> {
    bounded_field(tenant, 120, "tenant", false)?;
    if !file.tenant.is_empty() && file.tenant != tenant {
        return Err("declaration tenant differs from target tenant".into());
    }
    if file.service_accounts.is_empty() || file.service_accounts.len() > MAX_ACCOUNTS {
        return Err("declare between 1 and 128 service accounts".into());
    }
    let (mut accounts, mut identities, mut usernames, mut emails, mut outputs) = (
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
    );
    for d in &file.service_accounts {
        bounded_field(&d.identity, 512, "identity", false)?;
        bounded_field(&d.project, 120, "project", true)?;
        bounded_field(&d.reason, 2048, "reason", true)?;
        checked_scopes(&[], &d.scopes)?;
        if !identities.insert(d.identity.as_str()) {
            return Err("duplicate service identity declaration".into());
        }
        if !d.account.is_empty() {
            canonical_uuid(&d.account)?;
            if !accounts.insert(d.account.as_str()) {
                return Err("duplicate account declaration".into());
            }
        } else if d.provision.is_none() {
            return Err("account UUID or explicit provision block is required".into());
        }
        if let Some(p) = &d.provision {
            bounded_field(&p.username, 150, "username", false)?;
            bounded_field(&p.email, 255, "email", false)?;
            p.password_ref.validate()?;
            if !usernames.insert(p.username.to_ascii_lowercase())
                || !emails.insert(p.email.to_ascii_lowercase())
            {
                return Err("duplicate normalized account username or email".into());
            }
        }
        if d.api_keys.len() > MAX_KEYS {
            return Err("at most 32 keys may be declared per account".into());
        }
        let mut names = BTreeSet::new();
        for k in &d.api_keys {
            bounded_field(&k.name, 150, "key name", false)?;
            checked_scopes(&k.scopes, &d.scopes)?;
            if !names.insert(k.name.to_ascii_lowercase()) {
                return Err("duplicate normalized key name".into());
            }
            if !k.rotate_from.is_empty() {
                bounded_field(&k.rotate_from, 20, "rotation source", false)?;
            }
            let path = output_path(base, &k.secret_output)?;
            if !outputs.insert(path) {
                return Err("duplicate secret output path".into());
            }
        }
    }
    Ok(())
}

fn live_grant(g: &user_pb::ServiceAccountGrant, tenant: &str) -> Result<LiveGrant, String> {
    canonical_uuid(&g.user_id)?;
    if g.tenant_id != tenant || g.revision < 1 || !matches!(g.status.as_str(), "ACTIVE" | "REVOKED")
    {
        return Err("live grant has foreign or malformed authority fields".into());
    }
    bounded_field(&g.service_identity, 512, "stored identity", false)?;
    let scopes: Vec<String> = serde_json::from_str(&g.approved_scopes_json)
        .map_err(|_| "stored approved_scopes_json must be an array of strings")?;
    let scopes = checked_scopes(&[], &scopes)?;
    Ok(LiveGrant {
        account: g.user_id.clone(),
        identity: g.service_identity.clone(),
        project: g.project_id.clone(),
        scopes,
        active: g.status == "ACTIVE",
        revision: g.revision,
    })
}

fn req<T>(body: T, tenant: &str) -> Result<Request<T>, String> {
    let mut r = super::authz_cli::with_metadata(body);
    r.metadata_mut().insert(
        "x-tenant-id",
        tenant.parse().map_err(|_| "tenant is invalid metadata")?,
    );
    r.set_timeout(Duration::from_secs(30));
    Ok(r)
}

fn rpc_error(op: &str, status: Status) -> String {
    // Do not echo arbitrary server text from a credential-bearing request.
    // Retain only the stable typed reason; it is needed to distinguish a grant
    // owned by another account from a retryable transport failure.
    let reason = status
        .metadata()
        .get_bin("udb-error-detail-bin")
        .and_then(|raw| raw.to_bytes().ok())
        .and_then(|raw| udb::proto::ErrorDetail::decode(raw.as_ref()).ok())
        .map(|detail| detail.reason)
        .filter(|reason| {
            reason.starts_with("UDB_")
                && reason.len() <= 128
                && reason
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        })
        .map(|reason| format!(", {reason}"))
        .unwrap_or_default();
    format!(
        "{op} failed ({:?}{reason}); no later steps were applied",
        status.code(),
    )
}

fn context(tenant: &str, project: &str) -> common::RequestContext {
    common::RequestContext {
        tenant: Some(common::TenantContext {
            tenant_id: tenant.into(),
            project_id: project.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn load_grants(
    client: &mut AuthnServiceClient<Channel>,
    tenant: &str,
) -> Result<Vec<LiveGrant>, String> {
    let mut token = String::new();
    let (mut seen_tokens, mut accounts, mut identities) =
        (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    let mut live = vec![];
    for _ in 0..MAX_PAGES {
        if !seen_tokens.insert(token.clone()) {
            return Err("grant pagination repeated a token".into());
        }
        let page = client
            .list_service_account_grants(req(
                authn_pb::ListServiceAccountGrantsRequest {
                    tenant_id: tenant.into(),
                    page_size: 200,
                    page_token: token,
                },
                tenant,
            )?)
            .await
            .map_err(|s| rpc_error("list grants", s))?
            .into_inner();
        for g in page.grants {
            let g = live_grant(&g, tenant)?;
            if !accounts.insert(g.account.clone()) || !identities.insert(g.identity.clone()) {
                return Err("live grant inventory contains duplicate ownership".into());
            }
            live.push(g);
        }
        token = page.next_page_token;
        if token.is_empty() {
            return Ok(live);
        }
    }
    Err("grant inventory exceeded the bounded page limit".into())
}

fn verify_user(u: &user_pb::User, d: &DeclaredAccount, tenant: &str) -> Result<(), String> {
    canonical_uuid(&u.user_id)?;
    if u.tenant_id != tenant
        || u.project_id != d.project
        || u.account_kind != user_pb::AccountKind::ServiceAccount as i32
        || (!d.account.is_empty() && d.account != u.user_id)
    {
        return Err("account is not the declared tenant/project SERVICE_ACCOUNT".into());
    }
    if let Some(p) = &d.provision {
        if !u.username.eq_ignore_ascii_case(&p.username) || !u.email.eq_ignore_ascii_case(&p.email)
        {
            return Err(
                "existing account does not match the explicit provisioning declaration".into(),
            );
        }
    }
    if u.status != user_pb::UserStatus::Active as i32 {
        let attributes: BTreeMap<String, String> = serde_json::from_str(&u.profile_attributes_json)
            .map_err(|_| "malformed service account profile attributes")?;
        let managed = attributes.get("udb.identity.managed").map(String::as_str) == Some("v1");
        if d.provision.is_none()
            || !managed
            || u.status != user_pb::UserStatus::PendingVerification as i32
        {
            return Err("existing service account is not ACTIVE; no implicit reactivation".into());
        }
    }
    Ok(())
}

async fn load_user(
    client: &mut AuthnServiceClient<Channel>,
    d: &DeclaredAccount,
    tenant: &str,
) -> Result<Option<user_pb::User>, String> {
    let r = authn_pb::GetUserRequest {
        user_id: d.account.clone(),
        username: if d.account.is_empty() {
            d.provision
                .as_ref()
                .map(|p| p.username.clone())
                .unwrap_or_default()
        } else {
            String::new()
        },
        ..Default::default()
    };
    match client.get_user(req(r, tenant)?).await {
        Ok(r) => {
            let u = r.into_inner().user.ok_or("GetUser returned no account")?;
            verify_user(&u, d, tenant)?;
            Ok(Some(u))
        }
        Err(s) if s.code() == Code::NotFound && d.account.is_empty() => Ok(None),
        Err(s) => Err(rpc_error("get declared account", s)),
    }
}

async fn load_keys(
    client: &mut ApiKeyServiceClient<Channel>,
    account: &str,
    tenant: &str,
    project: &str,
) -> Result<Vec<key_entity::ApiKey>, String> {
    let mut keys = vec![];
    let (mut ids, mut active_names) = (BTreeSet::new(), BTreeSet::new());
    for page_no in 1..=MAX_PAGES {
        let r = client
            .list_api_keys(req(
                key_pb::ListApiKeysRequest {
                    owner_id: account.into(),
                    owner_type: key_entity::ApiKeyOwnerType::ServiceAccount as i32,
                    page: Some(common::PageRequest {
                        page: page_no as i32,
                        page_size: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                tenant,
            )?)
            .await
            .map_err(|s| rpc_error("list account keys", s))?
            .into_inner();
        for k in r.keys {
            if k.owner_id != account
                || k.tenant_id != tenant
                || k.project_id != project
                || k.owner_type != key_entity::ApiKeyOwnerType::ServiceAccount as i32
                || !ids.insert(k.key_id.clone())
                || !matches!(k.status, 1..=3)
            {
                return Err(
                    "key inventory contains duplicate, foreign or malformed ownership".into(),
                );
            }
            let scopes: Vec<String> = serde_json::from_str(&k.scopes_json)
                .map_err(|_| "stored key scopes must be an array of strings")?;
            checked_scopes(&[], &scopes)?;
            if k.status == key_entity::ApiKeyStatus::Active as i32
                && !active_names.insert(k.name.to_ascii_lowercase())
            {
                return Err("multiple ACTIVE keys have the same normalized name".into());
            }
            keys.push(k);
        }
        let page = r.page.ok_or("ListApiKeys returned no pagination receipt")?;
        if page.page != page_no as i32 || page.total_items < 0 {
            return Err("malformed key pagination receipt".into());
        }
        if !page.has_next {
            return Ok(keys);
        }
    }
    Err("key inventory exceeded the bounded page limit".into())
}

fn private_regular(path: &Path) -> Result<(), String> {
    #[cfg(not(unix))]
    {
        let _ = path;
        return Err("protected file input requires the owner-only Unix file adapter".into());
    }
    #[cfg(unix)]
    {
        let m = fs::symlink_metadata(path).map_err(|_| "protected file is unavailable")?;
        if !m.file_type().is_file() {
            return Err("protected file must be a regular file without a symlink".into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if m.permissions().mode() & 0o077 != 0 {
                return Err("protected file must have owner-only permissions".into());
            }
        }
        Ok(())
    }
}

fn read_private(path: &Path, limit: u64) -> Result<String, String> {
    private_regular(path)?;
    if fs::metadata(path)
        .map_err(|_| "protected file metadata unavailable")?
        .len()
        > limit
    {
        return Err("protected file exceeds its size limit".into());
    }
    fs::read_to_string(path).map_err(|_| "protected file could not be read".into())
}

fn output_path(base: &Path, raw: &str) -> Result<PathBuf, String> {
    bounded_field(raw, 4096, "secret_output", false)?;
    let path = base.join(raw);
    let parent = path
        .parent()
        .ok_or("secret output requires a parent directory")?;
    let parent = fs::canonicalize(parent)
        .map_err(|_| "secret output parent directory must already exist")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(&parent)
            .map_err(|_| "output directory unavailable")?
            .permissions()
            .mode()
            & 0o077
            != 0
        {
            return Err("secret output directory must have owner-only permissions".into());
        }
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        return Err("API-key secret output requires the owner-only Unix file adapter; no credential was created".into());
    }
    #[cfg(unix)]
    Ok(parent.join(path.file_name().ok_or("secret output needs a filename")?))
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct KeyReceipt {
    schema: u32,
    tenant: String,
    project: String,
    account: String,
    name: String,
    key_id: String,
    previous_key_id: String,
}

impl KeyReceipt {
    fn matches(&self, d: &DeclaredKey, account: &str, tenant: &str, project: &str) -> bool {
        self.schema == 1
            && self.tenant == tenant
            && self.project == project
            && self.account == account
            && self.name == d.name
            && self.previous_key_id == d.rotate_from
    }
}

fn publish_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    // Same-directory temporary file + exclusive hard-link publish: never
    // overwrite an operator file, and never expose a partially written secret.
    let tmp = sidecar(path, &format!(".{}.tmp", uuid::Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        let mut f = options
            .open(&tmp)
            .map_err(|_| "cannot create private output temporary file")?;
        f.write_all(bytes)
            .map_err(|_| "cannot write private output")?;
        f.sync_all().map_err(|_| "cannot sync private output")?;
        fs::hard_link(&tmp, path)
            .map_err(|_| "private output already exists or could not be published")?;
        #[cfg(unix)]
        {
            fs::File::open(path.parent().ok_or("missing output parent")?)
                .and_then(|d| d.sync_all())
                .map_err(|_| "cannot sync private output directory")?;
        }
        Ok(())
    })();
    let _ = fs::remove_file(&tmp);
    result
}

#[derive(Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum KeyStep {
    Unchanged { key_id: String },
    ReviewScopes { key_id: String },
    Create,
    Rotate { from: String },
}

async fn verify_secret(
    client: &mut ApiKeyServiceClient<Channel>,
    secret: &Secret,
    key: &key_entity::ApiKey,
    tenant: &str,
) -> Result<(), String> {
    if !secret.0.starts_with(&format!("{}.", key.key_id)) {
        return Err("saved secret is not bound to the returned key prefix".into());
    }
    let r = client
        .validate_api_key(req(
            key_pb::ValidateApiKeyRequest {
                plain_key: secret.0.clone(),
                endpoint: "udb.identity.preflight".into(),
                ..Default::default()
            },
            tenant,
        )?)
        .await
        .map_err(|s| rpc_error("validate saved key binding", s))?
        .into_inner();
    let scopes: Vec<String> =
        serde_json::from_str(&key.scopes_json).map_err(|_| "malformed key scope receipt")?;
    if !r.valid
        || r.rate_limited
        || r.key_id != key.key_id
        || r.owner_id != key.owner_id
        || r.owner_type != key.owner_type
        || sorted_scopes(&r.scopes) != sorted_scopes(&scopes)
    {
        return Err("saved secret failed the server's current owner/scope validation".into());
    }
    Ok(())
}

async fn plan_key(
    client: &mut ApiKeyServiceClient<Channel>,
    k: &DeclaredKey,
    account: &str,
    tenant: &str,
    project: &str,
    grant_scopes: &[String],
    current_grant: Option<&LiveGrant>,
    keys: &[key_entity::ApiKey],
    base: &Path,
) -> Result<KeyStep, String> {
    let out = output_path(base, &k.secret_output)?;
    let receipt_path = sidecar(&out, ".udb-receipt.json");
    let intent_path = sidecar(&out, ".udb-intent.json");
    if receipt_path
        .try_exists()
        .map_err(|_| "cannot inspect output receipt")?
    {
        let receipt: KeyReceipt = serde_json::from_str(&read_private(&receipt_path, 16384)?)
            .map_err(|_| "invalid output receipt")?;
        if !receipt.matches(k, account, tenant, project) {
            return Err("output receipt belongs to another declaration or owner".into());
        }
        let key = keys
            .iter()
            .find(|key| {
                key.key_id == receipt.key_id
                    && key.status == key_entity::ApiKeyStatus::Active as i32
            })
            .ok_or("receipt does not identify a current ACTIVE server key")?;
        if key.name != k.name {
            return Err("server key name differs from the saved output binding".into());
        }
        let secret = Secret(
            read_private(&out, 4096)?
                .trim_end_matches(['\r', '\n'])
                .into(),
        );
        if !secret.0.starts_with(&format!("{}.", key.key_id)) {
            return Err("saved secret is not bound to the returned key prefix".into());
        }
        let current_grant = current_grant
            .filter(|grant| grant.active)
            .ok_or("saved key requires the current ACTIVE account grant")?;
        let metadata: Value = serde_json::from_str(&key.metadata_json)
            .map_err(|_| "saved key has malformed grant metadata")?;
        let reviewed_revision = metadata
            .get("grant_revision")
            .and_then(Value::as_i64)
            .filter(|revision| *revision > 0 && *revision <= current_grant.revision)
            .ok_or("saved key has invalid grant revision metadata")?;
        let stale_grant = reviewed_revision < current_grant.revision;
        if !stale_grant {
            verify_secret(client, &secret, key, tenant).await?;
        }
        let wanted = checked_scopes(&k.scopes, grant_scopes)?;
        let actual: Vec<String> =
            serde_json::from_str(&key.scopes_json).map_err(|_| "invalid key scopes")?;
        // A stale key cannot currently authenticate. An explicit desired-scope
        // declaration may re-review it only through verified operator authority
        // and the native UpdateApiKey gate; validate the saved secret afterward.
        return Ok(
            if !stale_grant && sorted_scopes(&wanted) == sorted_scopes(&actual) {
                KeyStep::Unchanged {
                    key_id: key.key_id.clone(),
                }
            } else {
                KeyStep::ReviewScopes {
                    key_id: key.key_id.clone(),
                }
            },
        );
    }
    if intent_path
        .try_exists()
        .map_err(|_| "cannot inspect pending output intent")?
    {
        return Err(
            "one-time key outcome is pending; explicit operator recovery is required".into(),
        );
    }
    if out
        .try_exists()
        .map_err(|_| "cannot inspect secret output")?
        || fs::symlink_metadata(&out).is_ok()
    {
        return Err(
            "secret output already exists without a completed binding; nothing was changed".into(),
        );
    }
    let active = keys.iter().find(|key| {
        key.status == key_entity::ApiKeyStatus::Active as i32
            && key.name.eq_ignore_ascii_case(&k.name)
    });
    if k.rotate_from.is_empty() {
        if active.is_some() {
            return Err("existing ACTIVE key requires its protected output receipt; it will not be replaced implicitly".into());
        }
        Ok(KeyStep::Create)
    } else {
        let old = active
            .ok_or("rotation requires an existing ACTIVE key with the exact declared name")?;
        if old.key_id != k.rotate_from {
            return Err("rotation source is not the current declared owner's key".into());
        }
        let current: Vec<String> =
            serde_json::from_str(&old.scopes_json).map_err(|_| "invalid rotation source scopes")?;
        if sorted_scopes(&current) != sorted_scopes(&checked_scopes(&k.scopes, grant_scopes)?) {
            return Err(
                "review the existing key's scopes before requesting an explicit rotation".into(),
            );
        }
        Ok(KeyStep::Rotate {
            from: old.key_id.clone(),
        })
    }
}

struct AccountPlan {
    declared: DeclaredAccount,
    user: Option<user_pb::User>,
    password: Option<Secret>,
    steps: Vec<IdentityStep>,
    keys: Vec<KeyStep>,
}

pub(super) async fn identity_command(
    apply: bool,
    file: &str,
    tenant: &str,
    allow_transfer: bool,
) -> Result<Value, String> {
    if file.trim().is_empty() {
        return Err("provide -f <identities.yaml>".into());
    }
    let raw = fs::read_to_string(file).map_err(|_| "identity declaration is unavailable")?;
    if raw.len() > 2 * 1024 * 1024 {
        return Err("identity declaration exceeds the bounded size limit".into());
    }
    let parsed: IdentityFile = if file.ends_with(".json") {
        serde_json::from_str(&raw).map_err(|_| "invalid identity declaration")?
    } else {
        serde_yaml::from_str(&raw).map_err(|_| "invalid identity declaration")?
    };
    let tenant = [tenant, &parsed.tenant]
        .into_iter()
        .find(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var("UDB_TENANT_ID").ok())
        .ok_or("target tenant is required")?;
    let base = fs::canonicalize(
        Path::new(file)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
    .map_err(|_| "declaration directory is unavailable")?;
    validate_declarations(&parsed, &tenant, &base)?;
    let token = std::env::var("UDB_AUTH_TOKEN")
        .or_else(|_| std::env::var("UDB_BEARER_TOKEN"))
        .map_err(|_| "verified bearer is required")?;
    if token.trim().is_empty() || token.chars().any(char::is_control) || !token.is_ascii() {
        return Err("verified bearer reference is malformed".into());
    }
    let target = super::authz_cli::auth_target();
    let channel = tonic::transport::Endpoint::from_shared(target)
        .map_err(|_| "invalid auth target")?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .map_err(|_| "cannot connect to the native auth service")?;
    let mut auth = AuthnServiceClient::new(channel.clone());
    let mut key_client = ApiKeyServiceClient::new(channel);
    let principal = auth
        .authenticate(req(
            authn_pb::AuthnRequest {
                bearer_token: token,
                ..Default::default()
            },
            &tenant,
        )?)
        .await
        .map_err(|s| rpc_error("verify operator", s))?
        .into_inner()
        .principal
        .ok_or("Authenticate returned no verified principal")?;
    if principal.subject.is_empty()
        || (!principal.tenant_id.is_empty() && principal.tenant_id != tenant)
    {
        return Err("verified operator tenant differs from the declaration".into());
    }
    if !principal.project_id.is_empty()
        && parsed
            .service_accounts
            .iter()
            .any(|d| d.project != principal.project_id)
    {
        return Err("verified operator project differs from a declaration".into());
    }
    let live = load_grants(&mut auth, &tenant).await?;
    let mut plans = vec![];
    let mut actual_accounts = BTreeSet::new();
    for mut d in parsed.service_accounts {
        let user = load_user(&mut auth, &d, &tenant).await?;
        if let Some(u) = &user {
            d.account = u.user_id.clone();
            if !actual_accounts.insert(d.account.clone()) {
                return Err("declarations resolve to the same server account".into());
            }
        }
        let steps = plan_identity(&d, &live, allow_transfer);
        let password = if user.is_none() {
            Some(
                d.provision
                    .as_ref()
                    .ok_or("missing explicit account provisioning")?
                    .password_ref
                    .resolve(&base)?,
            )
        } else {
            None
        };
        let keys = if let Some(u) = &user {
            load_keys(&mut key_client, &u.user_id, &tenant, &d.project).await?
        } else {
            vec![]
        };
        let mut key_steps = vec![];
        for k in &d.api_keys {
            key_steps.push(
                plan_key(
                    &mut key_client,
                    k,
                    &d.account,
                    &tenant,
                    &d.project,
                    &d.scopes,
                    live.iter().find(|grant| grant.account == d.account),
                    &keys,
                    &base,
                )
                .await?,
            );
        }
        plans.push(AccountPlan {
            declared: d,
            user,
            password,
            steps,
            keys: key_steps,
        });
    }
    let refused = plans
        .iter()
        .flat_map(|p| &p.steps)
        .filter(|s| matches!(s, IdentityStep::Refused { .. }))
        .count();
    let report: Vec<Value> = plans.iter().map(|p| json!({ "account": p.declared.account, "identity": p.declared.identity,
        "account_action": match &p.user { None => "create", Some(u) if u.status != user_pb::UserStatus::Active as i32 => "activate_owned_pending", _ => "unchanged" },
        "steps": p.steps, "api_keys": p.declared.api_keys.iter().zip(&p.keys).map(|(k,s)| json!({"name":k.name,"step":s})).collect::<Vec<_>>() })).collect();
    if apply && refused != 0 {
        return Err("UDB_GRANT_OWNED_BY_OTHER or revoked grant: reconciliation refused before any mutation; run identity diff".into());
    }
    let mut results = vec![];
    if apply {
        for p in &mut plans {
            let d = &mut p.declared;
            let reason = if d.reason.is_empty() {
                "udb identity apply"
            } else {
                &d.reason
            };
            let mut user = match p.user.take() {
                Some(u) => u,
                None => {
                    let declared = d.provision.as_ref().ok_or("missing account declaration")?;
                    auth.create_user(req(
                        authn_pb::CreateUserRequest {
                            username: declared.username.clone(),
                            email: declared.email.clone(),
                            password: p.password.take().ok_or("missing password reference")?.0,
                            tenant_id: tenant.clone(),
                            project_id: d.project.clone(),
                            account_kind: user_pb::AccountKind::ServiceAccount as i32,
                            profile_attributes: BTreeMap::from([(
                                "udb.identity.managed".into(),
                                "v1".into(),
                            )])
                            .into_iter()
                            .collect(),
                            context: Some(context(&tenant, &d.project)),
                            ..Default::default()
                        },
                        &tenant,
                    )?)
                    .await
                    .map_err(|s| rpc_error("create service account", s))?
                    .into_inner()
                    .user
                    .ok_or("CreateUser returned no account")?
                }
            };
            verify_user(&user, d, &tenant)?;
            d.account = user.user_id.clone();
            if user.status != user_pb::UserStatus::Active as i32 {
                user = auth
                    .change_user_status(req(
                        authn_pb::ChangeUserStatusRequest {
                            user_id: d.account.clone(),
                            new_status: user_pb::UserStatus::Active as i32,
                            reason: reason.into(),
                            context: Some(context(&tenant, &d.project)),
                        },
                        &tenant,
                    )?)
                    .await
                    .map_err(|s| rpc_error("activate owned service account", s))?
                    .into_inner()
                    .user
                    .ok_or("activation returned no account")?;
                verify_user(&user, d, &tenant)?;
                if user.status != user_pb::UserStatus::Active as i32 {
                    return Err("server did not activate the declared account".into());
                }
            }
            let mut revision = 0;
            let mut grant_changed = false;
            for s in &p.steps {
                let grant = match s {
                    IdentityStep::Unchanged | IdentityStep::Refused { .. } => continue,
                    IdentityStep::Create => {
                        auth.create_service_account_grant(req(
                            authn_pb::CreateServiceAccountGrantRequest {
                                tenant_id: tenant.clone(),
                                user_id: d.account.clone(),
                                service_identity: d.identity.clone(),
                                project_id: d.project.clone(),
                                approved_scopes: checked_scopes(&[], &d.scopes)?,
                                reason: reason.into(),
                            },
                            &tenant,
                        )?)
                        .await
                        .map_err(|s| rpc_error("create grant", s))?
                        .into_inner()
                        .grant
                    }
                    IdentityStep::Transfer {
                        from,
                        expected_revision,
                    } => {
                        auth.transfer_service_account_grant(req(
                            authn_pb::TransferServiceAccountGrantRequest {
                                tenant_id: tenant.clone(),
                                from_user_id: from.clone(),
                                to_user_id: d.account.clone(),
                                expected_revision: *expected_revision,
                                reason: reason.into(),
                            },
                            &tenant,
                        )?)
                        .await
                        .map_err(|s| rpc_error("explicit grant transfer", s))?
                        .into_inner()
                        .grant
                    }
                    IdentityStep::RotateIdentity { expected_revision } => {
                        auth.rotate_service_account_identity(req(
                            authn_pb::RotateServiceAccountIdentityRequest {
                                tenant_id: tenant.clone(),
                                user_id: d.account.clone(),
                                new_service_identity: d.identity.clone(),
                                expected_revision: revision.max(*expected_revision),
                                reason: reason.into(),
                            },
                            &tenant,
                        )?)
                        .await
                        .map_err(|s| rpc_error("rotate grant identity", s))?
                        .into_inner()
                        .grant
                    }
                    IdentityStep::ReplaceScopes { expected_revision } => {
                        auth.replace_service_account_grant(req(
                            authn_pb::ReplaceServiceAccountGrantRequest {
                                tenant_id: tenant.clone(),
                                user_id: d.account.clone(),
                                approved_scopes: checked_scopes(&[], &d.scopes)?,
                                project_id: d.project.clone(),
                                expected_revision: revision.max(*expected_revision),
                                reason: reason.into(),
                            },
                            &tenant,
                        )?)
                        .await
                        .map_err(|s| rpc_error("review grant scopes", s))?
                        .into_inner()
                        .grant
                    }
                }
                .ok_or("grant mutation returned no durable grant")?;
                let grant = live_grant(&grant, &tenant)?;
                if grant.account != d.account || !grant.active {
                    return Err("grant mutation returned mismatched authority".into());
                }
                revision = grant.revision;
                grant_changed = true;
            }
            let final_grant = auth
                .get_service_account_grant(req(
                    authn_pb::GetServiceAccountGrantRequest {
                        tenant_id: tenant.clone(),
                        user_id: d.account.clone(),
                    },
                    &tenant,
                )?)
                .await
                .map_err(|s| rpc_error("verify final grant", s))?
                .into_inner()
                .grant
                .ok_or("grant read returned no record")?;
            let final_grant = live_grant(&final_grant, &tenant)?;
            if !final_grant.active
                || final_grant.account != d.account
                || final_grant.identity != d.identity
                || final_grant.project != d.project
                || sorted_scopes(&final_grant.scopes) != sorted_scopes(&d.scopes)
            {
                return Err("current durable grant differs from the applied declaration".into());
            }
            revision = final_grant.revision;
            for (k, s) in d.api_keys.iter().zip(&p.keys) {
                let scopes = checked_scopes(&k.scopes, &d.scopes)?;
                match s {
                    KeyStep::Unchanged { .. } | KeyStep::ReviewScopes { .. } => {
                        let id = match s {
                            KeyStep::Unchanged { key_id } | KeyStep::ReviewScopes { key_id } => {
                                key_id
                            }
                            _ => unreachable!(),
                        };
                        // A changed grant revision also needs explicit key re-review.
                        if !matches!(s, KeyStep::Unchanged { .. }) || grant_changed {
                            let key = key_client
                                .update_api_key(req(
                                    key_pb::UpdateApiKeyRequest {
                                        key_id: id.clone(),
                                        scopes,
                                        context: Some(context(&tenant, &d.project)),
                                        update_mask: Some(prost_types::FieldMask {
                                            paths: vec!["scopes".into()],
                                        }),
                                        ..Default::default()
                                    },
                                    &tenant,
                                )?)
                                .await
                                .map_err(|s| rpc_error("review key scopes", s))?
                                .into_inner()
                                .key
                                .ok_or("key update returned no key")?;
                            if key.owner_id != d.account
                                || key.tenant_id != tenant
                                || key.project_id != d.project
                            {
                                return Err("key update returned mismatched authority".into());
                            }
                            let out = output_path(&base, &k.secret_output)?;
                            let saved = Secret(
                                read_private(&out, 4096)?
                                    .trim_end_matches(['\r', '\n'])
                                    .into(),
                            );
                            verify_secret(&mut key_client, &saved, &key, &tenant).await?;
                        }
                    }
                    KeyStep::Create | KeyStep::Rotate { .. } => {
                        let out = output_path(&base, &k.secret_output)?;
                        let intent = json!({"schema":1,"tenant":tenant,"project":d.project,"account":d.account,"name":k.name,"previous_key_id":k.rotate_from});
                        publish_private(
                            &sidecar(&out, ".udb-intent.json"),
                            &serde_json::to_vec(&intent)
                                .map_err(|_| "cannot encode pending intent")?,
                        )?;
                        let (key, secret) = match s {
                            KeyStep::Create => {
                                let r = key_client
                                    .create_api_key(req(
                                        key_pb::CreateApiKeyRequest {
                                            name: k.name.clone(),
                                            owner_type: key_entity::ApiKeyOwnerType::ServiceAccount
                                                as i32,
                                            owner_id: d.account.clone(),
                                            scopes,
                                            context: Some(context(&tenant, &d.project)),
                                            ..Default::default()
                                        },
                                        &tenant,
                                    )?)
                                    .await
                                    .map_err(|s| {
                                        rpc_error("create key (pending intent retained)", s)
                                    })?
                                    .into_inner();
                                (
                                    r.key.ok_or(
                                        "key creation returned no record; pending intent retained",
                                    )?,
                                    Secret(r.plain_key),
                                )
                            }
                            KeyStep::Rotate { from } => {
                                let r = key_client
                                    .rotate_api_key(req(
                                        key_pb::RotateApiKeyRequest {
                                            key_id: from.clone(),
                                            rotation_reason: reason.into(),
                                            context: Some(context(&tenant, &d.project)),
                                        },
                                        &tenant,
                                    )?)
                                    .await
                                    .map_err(|s| {
                                        rpc_error("rotate key (pending intent retained)", s)
                                    })?
                                    .into_inner();
                                if r.previous_key_id != *from {
                                    return Err("rotation returned the wrong predecessor; pending intent retained".into());
                                }
                                (
                                    r.key.ok_or(
                                        "rotation returned no record; pending intent retained",
                                    )?,
                                    Secret(r.plain_key),
                                )
                            }
                            _ => unreachable!(),
                        };
                        if key.owner_id != d.account
                            || key.tenant_id != tenant
                            || key.project_id != d.project
                            || key.name != k.name
                            || key.status != key_entity::ApiKeyStatus::Active as i32
                        {
                            return Err(
                                "minted key has mismatched authority; pending intent retained"
                                    .into(),
                            );
                        }
                        verify_secret(&mut key_client, &secret, &key, &tenant).await?;
                        publish_private(&out, secret.0.as_bytes())?;
                        let receipt = KeyReceipt {
                            schema: 1,
                            tenant: tenant.clone(),
                            project: d.project.clone(),
                            account: d.account.clone(),
                            name: k.name.clone(),
                            key_id: key.key_id,
                            previous_key_id: k.rotate_from.clone(),
                        };
                        publish_private(
                            &sidecar(&out, ".udb-receipt.json"),
                            &serde_json::to_vec(&receipt)
                                .map_err(|_| "cannot encode completion receipt")?,
                        )?;
                    }
                }
            }
            results.push(json!({"account":d.account,"identity":d.identity,"revision":revision}));
        }
    }
    let declared: BTreeSet<_> = plans.iter().map(|p| p.declared.account.as_str()).collect();
    Ok(
        json!({"tenant_id":tenant,"applied":apply,"plan":report,"refused":refused,"results":results,
        "undeclared_active_grants":live.iter().filter(|g|g.active && !declared.contains(g.account.as_str())).map(|g|json!({"account":g.account,"identity":g.identity})).collect::<Vec<_>>() }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declared(account: &str, identity: &str, scopes: &[&str]) -> DeclaredAccount {
        DeclaredAccount {
            account: account.into(),
            identity: identity.into(),
            project: String::new(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            reason: String::new(),
            provision: None,
            api_keys: vec![],
        }
    }

    fn grant(account: &str, identity: &str, scopes: &[&str], revision: i64) -> LiveGrant {
        LiveGrant {
            account: account.into(),
            identity: identity.into(),
            project: String::new(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            active: true,
            revision,
        }
    }

    #[test]
    fn matching_grant_is_unchanged() {
        let live = [grant(
            "a",
            "billing",
            &["udb:data:read", "udb:data:write"],
            3,
        )];
        assert_eq!(
            plan_identity(
                &declared(
                    "a",
                    "billing",
                    &["udb:data:write", "udb:data:read", "udb:data:read"]
                ),
                &live,
                false
            ),
            vec![IdentityStep::Unchanged]
        );
    }

    #[test]
    fn identity_held_by_another_account_needs_allow_transfer() {
        let live = [grant("prod", "billing", &["udb:data:read"], 7)];
        assert!(
            matches!(&plan_identity(&declared("dev", "billing", &["udb:data:read"]), &live, false)[..], [IdentityStep::Refused { reason, .. }] if reason == "UDB_GRANT_OWNED_BY_OTHER")
        );
        assert_eq!(
            plan_identity(&declared("dev", "billing", &["udb:data:read"]), &live, true),
            vec![IdentityStep::Transfer {
                from: "prod".into(),
                expected_revision: 7
            }]
        );
    }

    #[test]
    fn create_rotate_and_replace() {
        assert_eq!(
            plan_identity(&declared("n", "new", &["udb:data:read"]), &[], false),
            vec![IdentityStep::Create]
        );
        assert_eq!(
            plan_identity(
                &declared("a", "new", &["udb:data:write"]),
                &[grant("a", "old", &["udb:data:read"], 2)],
                false
            ),
            vec![
                IdentityStep::RotateIdentity {
                    expected_revision: 2
                },
                IdentityStep::ReplaceScopes {
                    expected_revision: 2
                }
            ]
        );
    }

    #[test]
    fn malformed_live_scopes_are_refused_instead_of_becoming_empty() {
        for scopes in ["null", "{}", "[1]", "not json", "[]", "[\"udb:admin\"]"] {
            let g = user_pb::ServiceAccountGrant {
                user_id: "10000000-0000-0000-0000-000000000001".into(),
                tenant_id: "tenant".into(),
                service_identity: "billing".into(),
                status: "ACTIVE".into(),
                revision: 1,
                approved_scopes_json: scopes.into(),
                ..Default::default()
            };
            assert!(
                live_grant(&g, "tenant").is_err(),
                "malformed scopes must not become a mutation plan: {scopes}"
            );
        }
    }

    #[test]
    fn declaration_rejects_foreign_duplicate_and_plaintext_secret_fields() {
        let account = "10000000-0000-0000-0000-000000000001";
        let d = declared(account, "billing", &["data:read"]);
        let file = IdentityFile {
            tenant: "tenant-a".into(),
            service_accounts: vec![d.clone()],
        };
        assert!(validate_declarations(&file, "tenant-b", Path::new(".")).is_err());
        let duplicate = IdentityFile {
            tenant: "tenant-a".into(),
            service_accounts: vec![d.clone(), d],
        };
        assert!(validate_declarations(&duplicate, "tenant-a", Path::new(".")).is_err());
        assert!(serde_json::from_value::<IdentityFile>(json!({"tenant":"tenant-a","service_accounts":[{"identity":"billing","scopes":["data:read"],"provision":{"username":"billing","email":"billing@example.invalid","password":"plaintext"}}]})).is_err());
    }

    #[test]
    fn receipt_is_not_authority_for_another_owner_or_rotation() {
        let k = DeclaredKey {
            name: "runtime".into(),
            scopes: vec!["data:read".into()],
            secret_output: "key".into(),
            rotate_from: "old".into(),
        };
        let r = KeyReceipt {
            schema: 1,
            tenant: "tenant".into(),
            project: "project".into(),
            account: "account".into(),
            name: "runtime".into(),
            key_id: "new".into(),
            previous_key_id: "old".into(),
        };
        assert!(r.matches(&k, "account", "tenant", "project"));
        assert!(!r.matches(&k, "another", "tenant", "project"));
        assert!(!r.matches(&k, "account", "foreign", "project"));
        let changed = DeclaredKey {
            rotate_from: "different".into(),
            ..k
        };
        assert!(!r.matches(&changed, "account", "tenant", "project"));
    }

    #[test]
    fn typed_refusal_is_retained_without_server_credential_text() {
        let mut status = Status::unknown("credential-that-must-not-be-printed");
        let detail = udb::proto::ErrorDetail {
            reason: "UDB_GRANT_OWNED_BY_OTHER".into(),
            ..Default::default()
        };
        status.metadata_mut().insert_bin(
            "udb-error-detail-bin",
            tonic::metadata::MetadataValue::from_bytes(&detail.encode_to_vec()),
        );
        let error = rpc_error("create grant", status);
        assert!(error.contains("UDB_GRANT_OWNED_BY_OTHER"));
        assert!(!error.contains("credential-that-must-not-be-printed"));
        let mut status = Status::unknown("another secret");
        let detail = udb::proto::ErrorDetail {
            reason: "UDB_SECRET\nunsafe".into(),
            ..Default::default()
        };
        status.metadata_mut().insert_bin(
            "udb-error-detail-bin",
            tonic::metadata::MetadataValue::from_bytes(&detail.encode_to_vec()),
        );
        assert!(!rpc_error("create grant", status).contains("unsafe"));
    }

    #[cfg(unix)]
    #[test]
    fn private_publication_is_exclusive_and_refuses_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory =
            std::env::temp_dir().join(format!("udb-identity-file-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let path = output_path(&directory, "owned.key").unwrap();
        publish_private(&path, b"one-time-test-value").unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(publish_private(&path, b"replacement").is_err());
        assert_eq!(read_private(&path, 4096).unwrap(), "one-time-test-value");
        let link = directory.join("link.key");
        symlink(&path, &link).unwrap();
        assert!(read_private(&link, 4096).is_err());
        assert!(publish_private(&link, b"replacement").is_err());
        fs::remove_file(link).unwrap();
        fs::remove_file(path).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}
