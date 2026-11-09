// TypeScript tour of UDB's native control plane: log in as the bootstrapped admin
// on the real enterprise path, then make at least one real call to each of the 28
// native services, printing one numbered line per service.
//
// Same steps, same requests and same output lines as ../go/main.go (the
// reference) and ../python/main.py.
//
// Run (after examples/ts_enterprise/scripts/bootstrap.sh + serve.sh):
//
//   set -a; source ../../ts_enterprise/secrets/enterprise.env; set +a
//   npm install && npm start
import { randomBytes, randomUUID } from "crypto";
import { UdbProject } from "@udb_plus/sdk";

const TOTAL_SERVICES = 28;

const env = (key: string, def: string): string => process.env[key] || def;

const pkg = (svc: string) => `udb.core.${svc}.services.v1`;
const S = {
  authn: `${pkg("authn")}.AuthnService`,
  tenant: `${pkg("tenant")}.TenantService`,
  authz: `${pkg("authz")}.AuthzService`,
  apikey: `${pkg("apikey")}.ApiKeyService`,
  idp: `${pkg("idp")}.IdentityProviderService`,
  config: `${pkg("config")}.ConfigService`,
  lock: `${pkg("lock")}.LockService`,
  livequery: `${pkg("livequery")}.LiveQueryService`,
  cache: `${pkg("cache")}.CacheService`,
  scheduler: `${pkg("scheduler")}.SchedulerService`,
  workflow: `${pkg("workflow")}.WorkflowService`,
  webhook: `${pkg("webhook")}.WebhookService`,
  vault: `${pkg("vault")}.VaultService`,
  notification: `${pkg("notification")}.NotificationService`,
  metering: `${pkg("metering")}.MeteringService`,
  analytics: `${pkg("analytics")}.AnalyticsService`,
  backup: `${pkg("backup")}.BackupService`,
  control: `${pkg("control")}.ControlPlaneService`,
  storage: `${pkg("storage")}.StorageService`,
  asset: `${pkg("asset")}.AssetService`,
  search: `${pkg("search")}.SearchService`,
  embedding: `${pkg("embedding")}.EmbeddingService`,
  messaging: `${pkg("messaging")}.MessagingService`,
  room: `${pkg("webrtc")}.RoomService`,
  peer: `${pkg("webrtc")}.PeerService`,
  track: `${pkg("webrtc")}.TrackService`,
  turn: `${pkg("webrtc")}.TurnService`,
  signaling: `${pkg("webrtc")}.SignalingService`,
};

// ── errors ──────────────────────────────────────────────────────────────────

const GRPC_CODES = [
  "OK", "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "NOT_FOUND",
  "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION",
  "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DATA_LOSS",
  "UNAUTHENTICATED",
];
const FAILED_PRECONDITION = 9;
const ERROR_KIND_CAPABILITY = 1;

/** A refusal the service returned inside an OK response (ApiError). */
class BodyError extends Error {
  constructor(readonly rpc: string, readonly code: string, readonly detailMessage: string) {
    super(`${rpc} returned ${code}: ${detailMessage}`);
  }
}

/** A step that could not run because a step it depends on was skipped or failed. */
class DepError extends Error {
  constructor(message: string, readonly skipped: boolean) {
    super(message);
  }
}

function apiErr(rpc: string, e: any): void {
  if (e && e.code) throw new BodyError(rpc, e.code, e.message ?? "");
}

// Domain reasons a service uses when its backend or secret is not set up on this
// broker (UPLOAD_URL_UNAVAILABLE, TURN_NOT_CONFIGURED, ...).
const NOT_CONFIGURED = /(NOT_CONFIGURED|NOT_AVAILABLE|UNAVAILABLE)$/;

interface RpcInfo {
  rpc: boolean;
  code: number;
  message: string;
  reason: string;
  domain: string;
  capability: string;
  kind: number;
}

function inspect(err: any): RpcInfo {
  // Unary calls throw UdbError (cause = the gRPC ServiceError); stream errors are
  // the raw ServiceError with the UdbError attached as `.udb`.
  const ue = err?.udb ?? err;
  const raw = ue?.cause ?? err;
  if (typeof raw?.code !== "number") {
    return { rpc: false, code: 2, message: String(err?.message ?? err), reason: "", domain: "", capability: "", kind: 0 };
  }
  const detail = ue?.detail ?? {};
  const domain = raw.metadata?.get?.("error-reason")?.[0];
  return {
    rpc: true,
    code: raw.code,
    message: raw.details || raw.message || "",
    reason: detail.reason && detail.reason !== detail.capability_required ? detail.reason : "",
    domain: domain ? String(domain) : "",
    capability: detail.capability_required ?? "",
    kind: detail.kind ?? 0,
  };
}

/** The reason line when err is the broker saying "this service's backend is not
 *  configured here"; null for any other failure. */
function skipReason(err: any): string | null {
  if (err instanceof BodyError) {
    return NOT_CONFIGURED.test(err.code) ? `${err.code}: ${err.detailMessage}` : null;
  }
  const info = inspect(err);
  if (!info.rpc || info.code !== FAILED_PRECONDITION) return null;
  const capability = info.kind === ERROR_KIND_CAPABILITY || info.reason === "UDB_CAPABILITY_UNSUPPORTED";
  if (!capability && !NOT_CONFIGURED.test(info.domain)) return null;
  let reason = info.domain || info.reason || "UDB_CAPABILITY_UNSUPPORTED";
  if (info.capability) reason += ` (${info.capability})`;
  return `${reason}: ${info.message}`;
}

function describe(err: any): string {
  if (err instanceof BodyError) return err.message;
  const info = inspect(err);
  if (!info.rpc) return info.message;
  const code = GRPC_CODES[info.code] ?? String(info.code);
  const reason = info.domain || info.reason;
  return reason ? `${code} ${reason}: ${info.message}` : `${code}: ${info.message}`;
}

// ── tour ────────────────────────────────────────────────────────────────────

type Status = "ok" | "skipped" | "failed";

class Tour {
  n = 0;
  called = 0;
  skipped = 0;
  failures: string[] = [];
  status = new Map<string, Status>();
  roomId = "";
  peerId = "";
  readonly suffix = randomBytes(4).toString("hex");

  constructor(
    readonly udb: UdbProject,
    readonly tenant: string,
    readonly project: string,
    readonly actor: string,
  ) {}

  call(service: string, method: string, body: any): Promise<any> {
    return this.udb.nativeCore.unary(service, method, body, { deadlineMs: 15_000 });
  }

  async step(service: string, optional: boolean, fn: () => Promise<string>): Promise<void> {
    this.n++;
    const prefix = `${String(this.n).padStart(2, "0")} ${service}:`;
    try {
      const msg = await fn();
      this.called++;
      this.status.set(service, "ok");
      console.log(prefix, msg);
      return;
    } catch (err: any) {
      if (err instanceof DepError && err.skipped) {
        this.skipped++;
        this.status.set(service, "skipped");
        console.log(prefix, "skipped —", err.message);
        return;
      }
      const reason = optional ? skipReason(err) : null;
      if (reason !== null) {
        this.skipped++;
        this.status.set(service, "skipped");
        console.log(prefix, "skipped —", reason);
        return;
      }
      this.status.set(service, "failed");
      const line = describe(err);
      this.failures.push(`${String(this.n).padStart(2, "0")} ${service}: ${line}`);
      console.log(prefix, "FAILED —", line);
    }
  }

  needs(service: string): void {
    const st = this.status.get(service);
    if (st === "ok") return;
    if (st === "skipped") throw new DepError(`needs ${service}, which was skipped above`, true);
    throw new DepError(`needs ${service}, which failed above`, false);
  }

  // ── steps (one per native service) ─────────────────────────────────────────

  async authn(): Promise<string> {
    const got = await this.call(S.authn, "GetUser", { user_id: this.actor });
    return `GetUser -> ${JSON.stringify(got.user?.username ?? "")} (${got.user?.status})`;
  }

  async tenantSvc(): Promise<string> {
    await this.call(S.tenant, "UpdateTenantConfig", {
      tenant_id: this.tenant, config_key: "native_tour_max_users", config_value: "200", type: "NUMBER",
    });
    const cfg = await this.call(S.tenant, "GetTenantConfig", { tenant_id: this.tenant });
    apiErr("GetTenantConfig", cfg.error);
    const kv = (cfg.configs ?? []).find((c: any) => c.config_key === "native_tour_max_users");
    if (!kv) throw new Error("GetTenantConfig did not return native_tour_max_users");
    return `UpdateTenantConfig + GetTenantConfig -> native_tour_max_users=${kv.config_value}`;
  }

  async authz(): Promise<string> {
    const code = `native_tour_${this.suffix}`;
    const created = await this.call(S.authz, "CreateRole", {
      name: `Native tour ${this.suffix}`, description: "native-services tour role", created_by: this.actor,
      role_code: code, domain: this.tenant, tenant_id: this.tenant, project_id: this.project,
    });
    const roleId = created.role?.role_id ?? "";
    let check: any;
    let failure: unknown;
    try {
      const got = await this.call(S.authz, "GetRole", { role_id: roleId });
      if (got.role?.role_code !== code) {
        throw new Error(`GetRole returned role_code ${JSON.stringify(got.role?.role_code)}, want ${JSON.stringify(code)}`);
      }
      check = await this.call(S.authz, "CheckAccess", {
        user_id: this.actor, domain: this.tenant, tenant_id: this.tenant, project_id: this.project,
        object: "billing.v1.Invoice", action: "Select",
      });
    } catch (err) {
      failure = err;
    }
    try {
      await this.call(S.authz, "DeleteRole", { role_id: roleId, deleted_by: this.actor });
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return `CreateRole + GetRole + DeleteRole ok; CheckAccess(admin, Select billing.v1.Invoice) -> allowed=${Boolean(check.allowed)}`;
  }

  async apiKey(): Promise<string> {
    const tenantCtx = { tenant: { tenant_id: this.tenant, project_id: this.project } };
    // An API key belongs to a service account, and a service account needs an
    // active grant (its approved scopes) before it can hold a key.
    const name = `native-tour-svc-${this.suffix}`;
    const sa = await this.call(S.authn, "CreateUser", {
      username: name, email: `${name}@example.com`, password: "CorrectHorse1!",
      tenant_id: this.tenant, project_id: this.project, full_name: "Native tour service account",
      account_kind: "ACCOUNT_KIND_SERVICE_ACCOUNT",
    });
    const owner: string = sa.user?.user_id ?? "";
    const deactivate = () =>
      this.call(S.authn, "ChangeUserStatus", {
        user_id: owner, new_status: "USER_STATUS_DEACTIVATED", reason: "native tour cleanup", context: tenantCtx,
      });
    let failure: unknown;
    let valid: any;
    try {
      await this.call(S.authn, "ChangeUserStatus", {
        user_id: owner, new_status: "USER_STATUS_ACTIVE", reason: "native tour service account", context: tenantCtx,
      });
      await this.call(S.authn, "CreateServiceAccountGrant", {
        tenant_id: this.tenant, project_id: this.project, user_id: owner, service_identity: name,
        approved_scopes: ["data:read"], reason: "native tour API key",
      });
      const keyCtx = { user_id: owner, tenant: tenantCtx.tenant };
      const created = await this.call(S.apikey, "CreateApiKey", {
        name: `native-tour-key-${this.suffix}`, owner_id: owner, scopes: ["data:read"], context: keyCtx,
      });
      try {
        valid = await this.call(S.apikey, "ValidateApiKey", { plain_key: created.plain_key, required_scope: "data:read" });
        if (!valid.valid || valid.owner_id !== owner) {
          throw new Error(`ValidateApiKey valid=${Boolean(valid.valid)} owner=${JSON.stringify(valid.owner_id)}, want true/${JSON.stringify(owner)}`);
        }
      } catch (err) {
        failure = err;
      }
      try {
        await this.call(S.apikey, "RevokeApiKey", {
          key_id: created.key?.key_id, revoke_reason: "native tour cleanup", context: keyCtx,
        });
      } catch (err) {
        failure ??= err;
      }
    } catch (err) {
      failure ??= err;
    }
    try {
      await this.call(S.authn, "RevokeServiceAccountGrant", { tenant_id: this.tenant, user_id: owner, reason: "native tour cleanup" });
    } catch (err) {
      failure ??= err;
    }
    try {
      await deactivate();
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return `CreateApiKey(service account) + ValidateApiKey(data:read) -> valid=${Boolean(valid.valid)}; RevokeApiKey ok`;
  }

  async idp(): Promise<string> {
    const list = await this.call(S.idp, "ListProviders", {
      tenant_id: this.tenant, kind: "IDP_KIND_UNSPECIFIED", page: { page: 1, page_size: 20 },
    });
    return `ListProviders -> ${(list.providers ?? []).length} provider(s)`;
  }

  async config(): Promise<string> {
    const key = `native_tour.${this.suffix}`;
    const where = { tenant_id: this.tenant, project_id: this.project, environment: "prod", flag_key: key };
    const put = await this.call(S.config, "PutFlag", {
      ...where, value: { bool_value: true }, enabled: true, rollout_percentage: 100,
      rollout_context_key: "user_id", metadata_json: "{}",
    });
    apiErr("PutFlag", put.error);
    let got: any;
    let failure: unknown;
    try {
      got = await this.call(S.config, "GetFlag", where);
      apiErr("GetFlag", got.error);
    } catch (err) {
      failure = err;
    }
    try {
      const del = await this.call(S.config, "DeleteFlag", where);
      apiErr("DeleteFlag", del.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    if (!got.found || got.flag?.value?.bool_value !== true) {
      throw new Error(`GetFlag found=${Boolean(got.found)} value=${JSON.stringify(got.flag?.value)}, want the flag just written`);
    }
    return "PutFlag + GetFlag -> bool_value=true; DeleteFlag ok";
  }

  async lock(): Promise<string> {
    const name = `native-tour-lock-${this.suffix}`;
    const acq = await this.call(S.lock, "AcquireLock", {
      tenant_id: this.tenant, lock_name: name, owner_id: this.actor, lease_ttl_seconds: 60, metadata_json: "{}",
    });
    apiErr("AcquireLock", acq.error);
    if (!acq.acquired) throw new Error(`AcquireLock acquired=false: ${acq.message}`);
    let got: any;
    let rel: any;
    let failure: unknown;
    try {
      got = await this.call(S.lock, "GetLock", { tenant_id: this.tenant, lock_name: name });
      apiErr("GetLock", got.error);
    } catch (err) {
      failure = err;
    }
    try {
      rel = await this.call(S.lock, "ReleaseLock", {
        tenant_id: this.tenant, lock_name: name, owner_id: this.actor, fencing_token: acq.fencing_token,
      });
      apiErr("ReleaseLock", rel.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    if (!got.found) throw new Error("GetLock did not find the lock just acquired");
    return `AcquireLock -> fencing token issued; GetLock -> found=${Boolean(got.found)}; ReleaseLock -> released=${Boolean(rel.released)}`;
  }

  async liveQuery(): Promise<string> {
    // Hold a lock so the subscription's snapshot has a row to show.
    const name = `native-tour-live-${this.suffix}`;
    const acq = await this.call(S.lock, "AcquireLock", {
      tenant_id: this.tenant, lock_name: name, owner_id: this.actor, lease_ttl_seconds: 60, metadata_json: "{}",
    });
    apiErr("AcquireLock", acq.error);
    let rows = -1;
    let failure: unknown;
    const abort = new AbortController();
    try {
      const stream = this.udb.nativeCore.serverStream(S.livequery, "Subscribe", {
        tenant_id: this.tenant, message_type: "udb.core.lock.entity.v1.Lock", snapshot_limit: 10,
        filters: [{ field: "lock_name", op: "LIVE_QUERY_COMPARISON_EQ", value: name }],
      }, { signal: abort.signal });
      const timer = setTimeout(() => abort.abort(), 15_000);
      try {
        for await (const frame of stream) {
          apiErr("Subscribe", frame.error);
          if (frame.snapshot) {
            rows = Number(frame.snapshot.row_count ?? 0);
            break;
          }
        }
      } finally {
        clearTimeout(timer);
        abort.abort();
      }
      if (rows < 0) throw new Error("Subscribe ended before sending a snapshot");
    } catch (err) {
      failure = err;
    }
    try {
      const rel = await this.call(S.lock, "ReleaseLock", {
        tenant_id: this.tenant, lock_name: name, owner_id: this.actor, fencing_token: acq.fencing_token,
      });
      apiErr("ReleaseLock", rel.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return `Subscribe(udb.core.lock.entity.v1.Lock, lock_name=...) -> snapshot with ${rows} row(s)`;
  }

  async cache(): Promise<string> {
    const ns = `native-tour-${this.suffix}`;
    const created = await this.call(S.cache, "CreateNamespace", {
      tenant_id: this.tenant, namespace: ns, max_bytes: 1 << 20, default_ttl_seconds: 300,
    });
    apiErr("CreateNamespace", created.error);
    let got: any;
    let failure: unknown;
    try {
      const set = await this.call(S.cache, "Set", {
        tenant_id: this.tenant, namespace: ns, key: "greeting", value: Buffer.from("hello"), ttl_seconds: 300,
      });
      apiErr("Set", set.error);
      got = await this.call(S.cache, "Get", { tenant_id: this.tenant, namespace: ns, key: "greeting" });
      apiErr("Get", got.error);
    } catch (err) {
      failure = err;
    }
    try {
      const del = await this.call(S.cache, "DeleteNamespace", { tenant_id: this.tenant, namespace: ns, confirmation_token: ns });
      apiErr("DeleteNamespace", del.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    const value = Buffer.from(got.value ?? []).toString();
    if (!got.found || value !== "hello") {
      throw new Error(`Get found=${Boolean(got.found)} value=${JSON.stringify(value)}, want "hello"`);
    }
    return `CreateNamespace + Set + Get -> "hello"; DeleteNamespace ok`;
  }

  async scheduler(): Promise<string> {
    const created = await this.call(S.scheduler, "CreateJob", {
      tenant_id: this.tenant, project_id: this.project, name: `native-tour-${this.suffix}`,
      schedule_type: "CRON", cron_expression: "*/5 * * * *", payload: "{}",
      target_topic: "native_tour.scheduler", max_attempts: 3, backoff_seconds: 30,
    });
    apiErr("CreateJob", created.error);
    let got: any;
    let failure: unknown;
    try {
      got = await this.call(S.scheduler, "GetJob", { tenant_id: this.tenant, job_id: created.job_id });
      apiErr("GetJob", got.error);
    } catch (err) {
      failure = err;
    }
    try {
      const del = await this.call(S.scheduler, "DeleteJob", { tenant_id: this.tenant, job_id: created.job_id });
      apiErr("DeleteJob", del.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return `CreateJob(*/5 * * * *) + GetJob -> ${got.job?.status}; DeleteJob ok`;
  }

  async workflow(): Promise<string> {
    const started = await this.call(S.workflow, "StartWorkflow", {
      tenant_id: this.tenant, project_id: this.project, workflow_type: "native_tour.demo", total_steps: 1,
      payload: "{}", compensations: "[]", correlation_id: `native-tour-${this.suffix}`,
    });
    apiErr("StartWorkflow", started.error);
    let got: any;
    let failure: unknown;
    try {
      got = await this.call(S.workflow, "GetWorkflow", { tenant_id: this.tenant, workflow_id: started.workflow_id });
      apiErr("GetWorkflow", got.error);
    } catch (err) {
      failure = err;
    }
    try {
      const cancelled = await this.call(S.workflow, "CancelWorkflow", {
        tenant_id: this.tenant, workflow_id: started.workflow_id, reason: "native tour cleanup",
      });
      apiErr("CancelWorkflow", cancelled.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return `StartWorkflow + GetWorkflow -> ${got.workflow?.status}; CancelWorkflow ok`;
  }

  async webhook(): Promise<string> {
    const created = await this.call(S.webhook, "CreateEndpoint", {
      tenant_id: this.tenant, url: "https://example.com/udb-native-tour", topic_pattern: "native_tour.*",
      description: "native-services tour", max_attempts: 3, metadata_json: "{}",
    });
    apiErr("CreateEndpoint", created.error);
    let got: any;
    let failure: unknown;
    try {
      got = await this.call(S.webhook, "GetEndpoint", { tenant_id: this.tenant, endpoint_id: created.endpoint_id });
      apiErr("GetEndpoint", got.error);
    } catch (err) {
      failure = err;
    }
    try {
      const del = await this.call(S.webhook, "DeleteEndpoint", { tenant_id: this.tenant, endpoint_id: created.endpoint_id });
      apiErr("DeleteEndpoint", del.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return `CreateEndpoint + GetEndpoint -> ${got.endpoint?.url}; DeleteEndpoint ok`;
  }

  async vault(): Promise<string> {
    const path = `native-tour/${this.suffix}/db-password`;
    const put = await this.call(S.vault, "PutSecret", {
      tenant_id: this.tenant, secret_path: path, secret_value: "s3cr3t", expected_version: 0, metadata_json: "{}",
    });
    apiErr("PutSecret", put.error);
    let got: any;
    let failure: unknown;
    try {
      got = await this.call(S.vault, "GetSecret", { tenant_id: this.tenant, secret_path: path });
      apiErr("GetSecret", got.error);
    } catch (err) {
      failure = err;
    }
    try {
      const del = await this.call(S.vault, "DeleteSecret", { tenant_id: this.tenant, secret_path: path });
      apiErr("DeleteSecret", del.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    if (got.secret_value !== "s3cr3t") throw new Error("GetSecret did not return the value just written");
    return `PutSecret + GetSecret -> version ${got.version} round-trips; DeleteSecret ok`;
  }

  async notification(): Promise<string> {
    const event = `native_tour.${this.suffix}`;
    await this.call(S.notification, "UpsertTemplate", {
      event_type: event, channel: "NOTIFICATION_CHANNEL_EMAIL", locale: "en",
      subject_template: "Native tour", body_template: "Hello {{name}}", is_active: true,
    });
    const sent = await this.call(S.notification, "SendNotification", {
      event_type: event, recipient_id: this.actor, recipient_address: "native-tour@example.com",
      tenant_id: this.tenant, project_id: this.project, locale: "en",
      variables: { name: "UDB" }, channels: ["NOTIFICATION_CHANNEL_EMAIL"],
    });
    if (!(sent.logs ?? []).length) throw new Error("SendNotification returned no delivery log");
    const got = await this.call(S.notification, "GetNotification", { log_id: sent.logs[0].log_id });
    return `UpsertTemplate + SendNotification(EMAIL) + GetNotification -> ${got.log?.status}`;
  }

  async metering(): Promise<string> {
    const metric = `native_tour.${this.suffix}`;
    const rec = await this.call(S.metering, "RecordUsage", {
      tenant_id: this.tenant, principal_id: this.actor, method: metric, unit: "request", quantity: 3, metadata_json: "{}",
    });
    apiErr("RecordUsage", rec.error);
    const used = await this.call(S.metering, "QueryUsage", { tenant_id: this.tenant, metric, window_seconds: 3600 });
    apiErr("QueryUsage", used.error);
    if (Number(used.used) !== 3) throw new Error(`QueryUsage used=${used.used}, want 3`);
    return `RecordUsage(3) + QueryUsage(1h) -> used=${used.used}`;
  }

  async analytics(): Promise<string> {
    const acc = await this.call(S.analytics, "RecordPipelineMetric", {
      stage_name: `native_tour_${this.suffix}`, tenant_id: this.tenant, latency_ms: 12.5, is_success: true,
    });
    if (!acc.accepted) throw new Error("RecordPipelineMetric accepted=false");
    const through = await this.call(S.analytics, "GetThroughput", { tenant_id: this.tenant });
    if (Number(through.total_requests) < 1) {
      throw new Error(`GetThroughput total_requests=${through.total_requests}, want >= 1`);
    }
    return "RecordPipelineMetric + GetThroughput -> total_requests >= 1";
  }

  async backup(): Promise<string> {
    const name = `native-tour-${this.suffix}`;
    const put = await this.call(S.backup, "PutBackupPolicy", {
      tenant_id: this.tenant, policy_name: name, schedule_cron: "0 3 * * *",
      retention_days: 7, max_retained_backups: 3, enabled: true, metadata_json: "{}",
    });
    apiErr("PutBackupPolicy", put.error);
    let got: any;
    let failure: unknown;
    try {
      got = await this.call(S.backup, "GetBackupPolicy", { tenant_id: this.tenant, policy_name: name });
      apiErr("GetBackupPolicy", got.error);
    } catch (err) {
      failure = err;
    }
    try {
      const del = await this.call(S.backup, "DeleteBackupPolicy", { tenant_id: this.tenant, policy_name: name });
      apiErr("DeleteBackupPolicy", del.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return `PutBackupPolicy + GetBackupPolicy -> ${JSON.stringify(got.policy?.schedule_cron ?? "")} retention ${got.policy?.retention_days} days; DeleteBackupPolicy ok`;
  }

  async controlPlane(): Promise<string> {
    const list = await this.call(S.control, "ListNodeStates", {
      resource_type: "RESOURCE_TYPE_UNSPECIFIED", page: { page: 1, page_size: 50 },
      context: { tenant: { tenant_id: this.tenant } },
    });
    return `ListNodeStates -> ${(list.node_states ?? []).length} node state(s)`;
  }

  async storage(): Promise<string> {
    const reg = await this.call(S.storage, "RegisterUpload", {
      tenant_id: this.tenant, filename: `native-tour-${this.suffix}.txt`, content_type: "text/plain",
      file_type: "DOCUMENT", reference_id: randomUUID(), reference_type: "native_tour", size_bytes: 5,
      expires_in_minutes: 10,
    });
    const fileId: string = reg.file_id ?? "";
    // Without an object store the file row is still registered, but no upload URL
    // can be minted: the response carries UPLOAD_URL_UNAVAILABLE. Remove the row
    // either way, then report.
    let failure: unknown;
    let got: any;
    try {
      apiErr("RegisterUpload", reg.error);
      if (fileId) {
        got = await this.call(S.storage, "GetFile", { tenant_id: this.tenant, file_id: fileId });
        apiErr("GetFile", got.error);
      }
    } catch (err) {
      failure = err;
    }
    if (fileId) {
      try {
        const del = await this.call(S.storage, "DeleteFile", { tenant_id: this.tenant, file_id: fileId, reason: "native tour cleanup" });
        apiErr("DeleteFile", del.error);
      } catch (err) {
        failure ??= err;
      }
    }
    if (failure) throw failure;
    return `RegisterUpload -> presigned upload URL issued; GetFile -> ${got?.file?.status}; DeleteFile ok`;
  }

  async asset(): Promise<string> {
    const def = await this.call(S.asset, "CreatePipelineDefinition", {
      tenant_id: this.tenant, name: `native-tour-${this.suffix}`, description: "native-services tour pipeline",
      media_type: "application/json", steps: `[{"name":"extract","type":"EXTRACT"}]`, version: 1,
    });
    apiErr("CreatePipelineDefinition", def.error);
    const got = await this.call(S.asset, "GetPipelineDefinition", { tenant_id: this.tenant, definition_id: def.definition_id });
    apiErr("GetPipelineDefinition", got.error);
    const assets = await this.call(S.asset, "ListAssets", { tenant_id: this.tenant, page: 1, page_size: 20 });
    apiErr("ListAssets", assets.error);
    return `CreatePipelineDefinition + GetPipelineDefinition -> version ${got.definition?.version}; ListAssets -> ${(assets.assets ?? []).length} asset(s)`;
  }

  async search(): Promise<string> {
    const list = await this.call(S.search, "ListIndexes", { tenant_id: this.tenant, page_size: 50 });
    apiErr("ListIndexes", list.error);
    return `ListIndexes -> ${(list.indexes ?? []).length} index(es)`;
  }

  async embedding(): Promise<string> {
    const list = await this.call(S.embedding, "ListModels", { tenant_id: this.tenant, page_size: 50 });
    apiErr("ListModels", list.error);
    return `ListModels -> ${(list.models ?? []).length} model(s)`;
  }

  async messaging(): Promise<string> {
    const list = await this.call(S.messaging, "ListMessagingDevices", {
      tenant_id: this.tenant, subject_id: this.actor, page_size: 20,
    });
    apiErr("ListMessagingDevices", list.error);
    return `ListMessagingDevices(admin) -> ${(list.devices ?? []).length} device(s)`;
  }

  // The five WebRTC services share one room: RoomService creates it, PeerService
  // joins it, TrackService publishes into it, TurnService issues TURN credentials
  // for the peer, and SignalingService pings over the signalling stream and then
  // closes the room.

  async room(): Promise<string> {
    const created = await this.call(S.room, "CreateRoom", {
      tenant_id: this.tenant, name: `native-tour-${this.suffix}`, max_participants: 8, config: "{}", created_by: this.actor,
    });
    apiErr("CreateRoom", created.error);
    try {
      const got = await this.call(S.room, "GetRoom", { tenant_id: this.tenant, room_id: created.room_id });
      apiErr("GetRoom", got.error);
      this.roomId = created.room_id;
      return `CreateRoom + GetRoom -> ${got.room?.state}`;
    } catch (err) {
      await this.call(S.room, "CloseRoom", { tenant_id: this.tenant, room_id: created.room_id }).catch(() => undefined);
      throw err;
    }
  }

  async peer(): Promise<string> {
    this.needs("RoomService");
    const joined = await this.call(S.peer, "JoinRoom", {
      tenant_id: this.tenant, room_id: this.roomId, display_name: "tour-peer", metadata: "{}", user_agent: "native-tour",
    });
    apiErr("JoinRoom", joined.error);
    this.peerId = joined.peer?.peer_id ?? "";
    const list = await this.call(S.peer, "ListPeers", { tenant_id: this.tenant, room_id: this.roomId, page_size: 20 });
    apiErr("ListPeers", list.error);
    return `JoinRoom + ListPeers -> ${(list.peers ?? []).length} peer(s) in the room`;
  }

  async track(): Promise<string> {
    this.needs("PeerService");
    const pub = await this.call(S.track, "PublishTrack", {
      tenant_id: this.tenant, room_id: this.roomId, peer_id: this.peerId, kind: "audio", label: "mic",
      settings: "{}", metadata: "{}",
    });
    apiErr("PublishTrack", pub.error);
    let list: any;
    let failure: unknown;
    try {
      list = await this.call(S.track, "ListTracks", { tenant_id: this.tenant, room_id: this.roomId, page_size: 20 });
      apiErr("ListTracks", list.error);
    } catch (err) {
      failure = err;
    }
    try {
      const unpub = await this.call(S.track, "UnpublishTrack", { tenant_id: this.tenant, track_id: pub.track_id });
      apiErr("UnpublishTrack", unpub.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return `PublishTrack(audio) + ListTracks -> ${(list.tracks ?? []).length} track(s); UnpublishTrack ok`;
  }

  async turn(): Promise<string> {
    this.needs("PeerService");
    const creds = await this.call(S.turn, "IssueCredentials", {
      tenant_id: this.tenant, room_id: this.roomId, peer_id: this.peerId, ttl_seconds: 3600,
    });
    apiErr("IssueCredentials", creds.error);
    return `IssueCredentials -> ${(creds.ice_servers ?? []).length} ICE server(s), ttl ${creds.ttl_seconds}s`;
  }

  async signaling(): Promise<string> {
    this.needs("RoomService");
    let failure: unknown;
    try {
      // Closing a signalling stream disconnects its peer, so ping with a second,
      // disposable peer.
      const joined = await this.call(S.peer, "JoinRoom", {
        tenant_id: this.tenant, room_id: this.roomId, display_name: "tour-signal", metadata: "{}", user_agent: "native-tour",
      });
      apiErr("JoinRoom", joined.error);
      const abort = new AbortController();
      const stream = this.udb.nativeCore.bidiStream(S.signaling, "Signal", { signal: abort.signal });
      try {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error("Signal: no pong within 15s")), 15_000);
          stream.on("data", (frame: any) => {
            if (frame.pong) {
              clearTimeout(timer);
              resolve();
            }
          });
          stream.on("error", (err: any) => {
            clearTimeout(timer);
            reject(err);
          });
          stream.on("end", () => {
            clearTimeout(timer);
            reject(new Error("Signal stream ended before a pong"));
          });
          stream.write({ tenant_id: this.tenant, room_id: this.roomId, peer_id: joined.peer?.peer_id, ping: true });
        });
      } finally {
        stream.end();
        abort.abort();
      }
    } catch (err) {
      failure = err;
    }
    // Leave + close the shared room (this step runs last).
    if (this.peerId) {
      try {
        const left = await this.call(S.peer, "LeaveRoom", { tenant_id: this.tenant, room_id: this.roomId, peer_id: this.peerId });
        apiErr("LeaveRoom", left.error);
      } catch (err) {
        failure ??= err;
      }
    }
    try {
      const closed = await this.call(S.room, "CloseRoom", { tenant_id: this.tenant, room_id: this.roomId });
      apiErr("CloseRoom", closed.error);
    } catch (err) {
      failure ??= err;
    }
    if (failure) throw failure;
    return "Signal(ping) -> pong; LeaveRoom + CloseRoom ok";
  }
}

async function main(): Promise<number> {
  const password = process.env.UDB_PASS ?? "";
  if (!password) {
    console.error("set UDB_PASS to the admin password bootstrap.sh wrote to secrets/enterprise.env");
    return 2;
  }
  const tenantCode = env("UDB_TENANT", "acme");
  const username = env("UDB_USER", "admin");

  let udb: UdbProject;
  try {
    udb = await UdbProject.connectEnterprise({
      target: env("UDB_TARGET", "127.0.0.1:50051"),
      authTarget: env("UDB_AUTH_TARGET", "127.0.0.1:50061"),
      tenantCode,
      projectId: env("UDB_PROJECT", "default"),
      purpose: "native-services-tour",
      username,
      password,
    });
  } catch (err: any) {
    console.error(`login failed: ${err?.message ?? err}`);
    return 1;
  }

  try {
    const principal = udb.principal() ?? {};
    const t = new Tour(udb, udb.canonicalTenantId, udb.canonicalProjectId, principal.user_id ?? "");
    console.log(`logged in as ${username} (tenant code ${JSON.stringify(tenantCode)} -> ${t.tenant})`);

    await t.step("AuthnService", false, () => t.authn());
    await t.step("TenantService", false, () => t.tenantSvc());
    await t.step("AuthzService", false, () => t.authz());
    await t.step("ApiKeyService", false, () => t.apiKey());
    await t.step("IdentityProviderService", false, () => t.idp());
    await t.step("ConfigService", false, () => t.config());
    await t.step("LockService", false, () => t.lock());
    await t.step("LiveQueryService", false, () => t.liveQuery());
    await t.step("CacheService", true, () => t.cache());
    await t.step("SchedulerService", false, () => t.scheduler());
    await t.step("WorkflowService", false, () => t.workflow());
    await t.step("WebhookService", false, () => t.webhook());
    await t.step("VaultService", true, () => t.vault());
    await t.step("NotificationService", true, () => t.notification());
    await t.step("MeteringService", true, () => t.metering());
    await t.step("AnalyticsService", false, () => t.analytics());
    await t.step("BackupService", false, () => t.backup());
    await t.step("ControlPlaneService", false, () => t.controlPlane());
    await t.step("StorageService", true, () => t.storage());
    await t.step("AssetService", true, () => t.asset());
    await t.step("SearchService", true, () => t.search());
    await t.step("EmbeddingService", true, () => t.embedding());
    await t.step("MessagingService", true, () => t.messaging());
    await t.step("RoomService", true, () => t.room());
    await t.step("PeerService", true, () => t.peer());
    await t.step("TrackService", true, () => t.track());
    await t.step("TurnService", true, () => t.turn());
    await t.step("SignalingService", true, () => t.signaling());

    console.log();
    if (t.failures.length > 0) {
      console.log(`FAILED ${t.failures.length}/${TOTAL_SERVICES} services (${t.called} called, ${t.skipped} skipped):`);
      for (const f of t.failures) console.log(`  ${f}`);
      return 1;
    }
    console.log(`OK ${t.called}/${TOTAL_SERVICES} services called, ${t.skipped} skipped (backend not configured)`);
    return 0;
  } finally {
    udb.close();
  }
}

main().then(
  (code) => process.exit(code),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
