"use strict";
// An in-memory udb broker for unit tests.
//
// It serves the DataBroker data verbs (Select, Insert, Upsert, Update, Delete),
// atomic transactions, durable CDC consumers and operator DLQ APIs, plus the API-key
// exchange, over a real loopback gRPC listener, so the code under test uses an
// unmodified UdbProject. Its semantics follow the broker's: the caller's tenant is
// filled into filters and records (a different tenant is refused), primary keys are
// unique, conditional writes compare by value, require_affected is enforced, reads
// page with has_more and exact totals, and every refusal carries the broker's stable
// UDB_* reason in the typed error detail.
//
// The same contract (conformance.ts) runs against this fake and a real broker,
// which keeps the two from drifting.
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Fake = void 0;
exports.createFake = createFake;
exports.createDescriptorFake = createDescriptorFake;
const node_crypto_1 = require("node:crypto");
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
const grpc = __importStar(require("@grpc/grpc-js"));
const protoLoader = __importStar(require("@grpc/proto-loader"));
const project_1 = require("../project");
const protoRoot_1 = require("../protoRoot");
const protoSchema_1 = require("../protoSchema");
const recordCodec_1 = require("../recordCodec");
const wkt_1 = require("../wkt");
const entityRules_1 = require("./entityRules");
const filter_1 = require("./filter");
const policy_1 = require("./policy");
const refusal_1 = require("./refusal");
const codes = grpc.status;
function wildcard(pattern, value) {
    const trimmed = (pattern ?? "").trim();
    return trimmed === "" || trimmed === "*" || trimmed === value;
}
function policyPattern(pattern, value) {
    const trimmed = (pattern ?? "").trim();
    if (wildcard(trimmed, value))
        return true;
    if (trimmed.endsWith(".*")) {
        const prefix = trimmed.slice(0, -2);
        return value === prefix || value.startsWith(`${prefix}.`);
    }
    return false;
}
function hasScope(identity, scope) {
    if (scope.trim() === "")
        return true;
    return (identity.scopes ?? []).some((granted) => granted === scope || granted === "*" || granted === "udb:*");
}
function topicMatches(pattern, topic) {
    if (pattern === "*" || pattern === topic)
        return true;
    return pattern.endsWith("*") && topic.startsWith(pattern.slice(0, -1));
}
function topicPattern(pattern) {
    const trimmed = (pattern ?? "").trim();
    return trimmed === "" ? "*" : trimmed;
}
function consumerOwner(identity) {
    if (identity.serviceIdentity)
        return `service:${identity.serviceIdentity}`;
    if (identity.userId)
        return `user:${identity.userId}`;
    throw (0, refusal_1.refuse)(codes.UNAUTHENTICATED, "", "named CDC consumer requires a verified user or service identity");
}
function consumerName(raw) {
    const name = raw.trim();
    if (name.length < 1 || name.length > 120)
        throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "invalid consumer_name length");
    if (!/^[A-Za-z0-9_.:-]+$/.test(name))
        throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "invalid consumer_name character");
    return name;
}
function cursorKey(identity, consumer, pattern) {
    return [identity.tenantId, identity.projectId ?? "", consumer, pattern].join("\0");
}
function cursorOwnerError() {
    return (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "named CDC consumer belongs to another verified principal");
}
function eventInScope(event, identity, pattern) {
    const project = typeof event.envelope.project_id === "string" ? event.envelope.project_id : "";
    return event.envelope.tenant_id === identity.tenantId && project === (identity.projectId ?? "") && topicMatches(pattern, event.topic);
}
function sameDomain(a, b) {
    return a.tenantId === b.tenantId && (a.projectId ?? "") === (b.projectId ?? "");
}
function missingScopeRefusal(scope) {
    return (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_SCOPE_MISSING", `verified identity lacks ${scope}`, {
        reason: "UDB_SCOPE_MISSING",
        kind: "ERROR_KIND_PERMISSION",
        missing: { scope },
    });
}
function bindBody(identity, body) {
    const tenant = body?.tenant_id;
    if (tenant && tenant !== identity.tenantId) {
        throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "body tenant differs from verified tenant");
    }
    const project = body?.project_id;
    if (project && project !== (identity.projectId ?? "")) {
        throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "body project differs from verified project");
    }
}
function struct(value) {
    return value ? (0, wkt_1.structToObject)(value) : {};
}
function hasFields(value) {
    return Object.keys(value?.fields ?? {}).length > 0;
}
function decodeObject(raw) {
    let value;
    try {
        value = (0, recordCodec_1.parseExactJson)(Buffer.from((raw ?? Buffer.alloc(0))).toString("utf8"));
    }
    catch {
        throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "record_json must be a JSON object");
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "record_json must be a JSON object");
    }
    return value;
}
const DEFINITIONS = new Map();
function loadedServices(root) {
    let loaded = DEFINITIONS.get(root);
    if (loaded)
        return loaded;
    const files = [];
    const walk = (dir) => {
        for (const entry of node_fs_1.default.readdirSync(dir, { withFileTypes: true })) {
            const full = node_path_1.default.join(dir, entry.name);
            if (entry.isDirectory())
                walk(full);
            else if (entry.name.endsWith(".proto"))
                files.push(full);
        }
    };
    walk(node_path_1.default.join(root, "udb"));
    loaded = grpc.loadPackageDefinition(protoLoader.loadSync(files, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
        includeDirs: [root, node_path_1.default.resolve(root, "../third_party/googleapis")],
    }));
    DEFINITIONS.set(root, loaded);
    return loaded;
}
/** The in-memory broker. */
class Fake {
    schemas;
    protoRoot;
    entities = new Map();
    // message type → tenant → primary-key string → row.
    rowsByType = new Map();
    eventLog = [];
    cursors = new Map();
    dlq = new Map();
    receipts = new Map();
    revisions = new Map();
    identities = new Map();
    tokens = new Map();
    faults = [];
    tokenLifetimeMs = 3_600_000;
    policies = [];
    defaultAllow = false;
    tuples = [];
    rate = { limit: 0, windowMs: 0, perPrincipal: false, buckets: new Map() };
    server;
    clients = [];
    emitted = 0;
    address = "";
    /** Use {@link createFake} or {@link createDescriptorFake}. */
    constructor(schemas, protoRoot) {
        this.schemas = schemas;
        this.protoRoot = protoRoot;
    }
    /** Add an entity message type. Its annotations define keys and the tenant column. */
    register(messageType) {
        this.entities.set(messageType, new entityRules_1.EntityRules(this.schemas.message(messageType), this.schemas));
    }
    /** The fake's gRPC address. */
    get target() {
        return this.address;
    }
    /** @internal */
    configure(policies, defaultAllow) {
        this.policies = policies;
        this.defaultAllow = defaultAllow;
    }
    /** Make `key` exchangeable for a bearer of `tenant`. */
    addApiKey(key, tenant) {
        this.addIdentityApiKey(key, {
            tenantId: tenant,
            serviceIdentity: `udbtest-service-${tenant}`,
            scopes: ["udb:read", "udb:write", "udb:admin", "udb:cdc:read", "udb:cdc:subscribe"],
        });
    }
    /** Provision immutable verified identity attributes. */
    addIdentityApiKey(key, identity) {
        this.identities.set(key, {
            ...identity,
            scopes: [...(identity.scopes ?? [])],
            roles: [...(identity.roles ?? [])],
            subject: identity.subject || identity.userId || identity.serviceIdentity,
        });
    }
    /** Connect a client for `tenant` with an exchanged API key, as a service would. It
     *  closes with the fake. */
    client(tenant) {
        const key = `udbtest-key-${tenant}`;
        this.addApiKey(key, tenant);
        return this.clientForKey(key);
    }
    /** Connect with a provisioned key without replacing its identity. */
    async clientForKey(key) {
        const identity = this.identities.get(key);
        if (!identity)
            throw new Error("udbtest: key is not provisioned");
        const client = await project_1.UdbProject.connect({
            target: this.address,
            tenantId: identity.tenantId,
            projectId: identity.projectId ?? "",
            purpose: "test",
            credentials: { apiKey: key },
            protoRoot: this.protoRoot,
        });
        this.clients.push(client);
        return client;
    }
    /** A copy of every stored row of `messageType` for `tenant`, for assertions. */
    rows(messageType, tenant) {
        const table = this.rowsByType.get(messageType)?.get(tenant);
        return [...(table?.values() ?? [])].map((row) => (0, entityRules_1.copyValue)(row)).sort((a, b) => ((0, recordCodec_1.recordToJson)(a) < (0, recordCodec_1.recordToJson)(b) ? -1 : 1));
    }
    /** The outbox events committed so far, in commit order. */
    events() {
        return this.eventLog.map((event) => ({ ...event, envelope: (0, entityRules_1.copyValue)(event.envelope) }));
    }
    /** Append an event as if a transaction of `tenant` had committed it, for tests that
     *  only exercise a consumer. Returns its event id. */
    emit(tenant, topic, partitionKey, payload) {
        this.emitted += 1;
        const id = `0190f1b2-0000-4000-8000-${this.emitted.toString(16).padStart(12, "0")}`;
        this.eventLog.push({
            topic,
            partitionKey,
            envelope: {
                event_id: id,
                event_type: topic,
                tenant_id: tenant,
                document_id: partitionKey,
                correlation_id: id,
                envelope_version: 2,
                payload: (0, entityRules_1.copyValue)(payload),
                project_id: "",
            },
        });
        return id;
    }
    /** Fail the next matching calls with `fault.code`. */
    injectFault(fault) {
        if (fault.code === codes.OK || (fault.times ?? 0) < 0 || (fault.retryAfterMs ?? 0) < 0) {
            throw new Error("udbtest: fault needs a failure code and non-negative count/delay");
        }
        const method = fault.method?.includes("/") ? fault.method.slice(fault.method.lastIndexOf("/") + 1) : fault.method;
        this.faults.push({ ...fault, method, times: fault.times || 1 });
    }
    /** Tune subsequently issued bearers, including refreshes. Existing bearers keep
     *  their expiry, as they do in production. */
    setTokenLifetime(lifetimeMs) {
        if (lifetimeMs < 1000)
            throw new Error("udbtest: token lifetime must be at least one second");
        this.tokenLifetimeMs = lifetimeMs;
    }
    /** The broker's default tenant/operation fixed-window bucket. Zero disables it. */
    setRateLimit(limit, windowMs) {
        if (limit < 0 || (limit > 0 && windowMs <= 0))
            throw new Error("udbtest: invalid rate limit");
        this.rate = { limit, windowMs, perPrincipal: false, buckets: new Map() };
    }
    /** The broker's optional per-principal bucket. */
    setRateLimitPerPrincipal(limit, windowMs) {
        this.setRateLimit(limit, windowMs);
        this.rate.perPrincipal = true;
    }
    /** Provision the scoped tuple the broker's relationship store consults. */
    addRelationship(subject, relation, object, tenant, project) {
        this.tuples.push({ subject, relation, object, tenant, project });
    }
    // ── Serving ───────────────────────────────────────────────────────────────
    /** @internal */
    async serve() {
        const loaded = loadedServices(this.protoRoot);
        const server = new grpc.Server();
        const unary = (method, handler) => (call, callback) => {
            try {
                const ctx = this.beforeRpc(method, call.metadata, call.request);
                callback(null, handler(ctx, call.request));
            }
            catch (err) {
                callback((0, refusal_1.statusOf)(err));
            }
        };
        server.addService(loaded.udb.services.v1.DataBroker.service, {
            Select: unary("Select", (ctx, r) => this.select(ctx, r)),
            Upsert: unary("Upsert", (ctx, r) => this.keyedWrite(ctx, "upsert", r, () => this.write(ctx.identity, r, false))),
            Insert: unary("Insert", (ctx, r) => this.keyedWrite(ctx, "insert", r, () => this.write(ctx.identity, r, true))),
            Update: unary("Update", (ctx, r) => this.keyedWrite(ctx, "update", r, () => this.update(ctx.identity, r))),
            Delete: unary("Delete", (ctx, r) => this.keyedWrite(ctx, "delete", r, () => this.remove(ctx.identity, r))),
            AckCdcEvents: unary("AckCdcEvents", (ctx, r) => this.settle(ctx, r)),
            NackCdcEvents: unary("NackCdcEvents", (ctx, r) => this.nack(ctx, r)),
            ListDlqEvents: unary("ListDlqEvents", (ctx, r) => this.listDlq(ctx, r)),
            GetDlqEvent: unary("GetDlqEvent", (ctx, r) => ({ event: { ...this.dlqEntry(ctx, r.dlq_id).record } })),
            ReplayDlqEvent: unary("ReplayDlqEvent", (ctx, r) => this.replayDlq(ctx, r)),
            DismissDlqEvent: unary("DismissDlqEvent", (ctx, r) => this.setDlqStatus(ctx, r, "DISMISSED")),
            QuarantineDlqEvent: unary("QuarantineDlqEvent", (ctx, r) => this.setDlqStatus(ctx, r, "QUARANTINED")),
            BeginTx: (call) => this.beginTx(call),
            PublishCDC: (call) => this.publishCdc(call),
        });
        server.addService(loaded.udb.core.authn.services.v1.AuthnService.service, {
            Authenticate: (call, callback) => {
                try {
                    this.admit("Authenticate", {}, call.request);
                    callback(null, this.authenticate(call.request.api_key));
                }
                catch (err) {
                    callback((0, refusal_1.statusOf)(err));
                }
            },
        });
        const port = await new Promise((resolve, reject) => {
            server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, bound) => (err ? reject(err) : resolve(bound)));
        });
        this.server = server;
        this.address = `127.0.0.1:${port}`;
    }
    /** Stop the listener and the clients it handed out. */
    close() {
        for (const client of this.clients.splice(0))
            client.close();
        this.server?.forceShutdown();
        this.server = undefined;
    }
    entity(messageType) {
        const entity = this.entities.get(messageType);
        if (!entity) {
            throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "UDB_UNKNOWN_MESSAGE_TYPE", `unknown message_type ${messageType}: the fake serves no such entity`);
        }
        if (entity.schemaError)
            throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "", entity.schemaError);
        return entity;
    }
    beforeRpc(method, metadata, request) {
        const authorization = metadata.get("authorization");
        if (authorization.length !== 1 || !String(authorization[0]).startsWith("Bearer ")) {
            throw (0, refusal_1.refuse)(codes.UNAUTHENTICATED, "", "a verified bearer is required");
        }
        const token = this.tokens.get(String(authorization[0]).slice("Bearer ".length));
        if (!token || Date.now() >= token.expires)
            throw (0, refusal_1.refuse)(codes.UNAUTHENTICATED, "", "bearer is unknown or expired");
        const identity = token.identity;
        for (const key of ["x-tenant-id", "x-udb-tenant-id"]) {
            const supplied = metadata.get(key);
            if (supplied.length > 1 || (supplied.length === 1 && supplied[0] !== identity.tenantId)) {
                throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "tenant header differs from verified tenant");
            }
        }
        for (const key of ["x-project-id", "x-udb-project-id"]) {
            const supplied = metadata.get(key);
            if (supplied.length > 1 || (supplied.length === 1 && supplied[0] !== (identity.projectId ?? ""))) {
                throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "project header differs from verified project");
            }
        }
        const ctx = { identity, token, metadata };
        if (request) {
            bindBody(identity, request.context);
            if (["ListDlqEvents", "GetDlqEvent", "ReplayDlqEvent", "DismissDlqEvent", "QuarantineDlqEvent"].includes(method) && !hasScope(identity, "udb:admin")) {
                throw missingScopeRefusal("udb:admin");
            }
            if (typeof request.message_type === "string") {
                this.authorize(ctx, request.message_type, method === "Insert" ? "Upsert" : method);
            }
        }
        this.admit(method, identity, request);
        return ctx;
    }
    authorize(ctx, resource, action) {
        const purposes = ctx.metadata.get("x-purpose");
        const purpose = purposes.length ? String(purposes[0]) : "";
        if (purpose.trim() === "")
            throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "purpose is required");
        if (!this.allows(ctx.identity, resource, action, purpose)) {
            throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "no matching allow or an explicit deny policy", {
                reason: "UDB_POLICY_DENIED",
                kind: "ERROR_KIND_POLICY",
                missing: { rule: `${action} ${resource}`, purpose },
            });
        }
    }
    allows(identity, resource, action, purpose) {
        let allowed = this.policies.length === 0 && this.defaultAllow;
        for (const policy of this.policies) {
            if (policy.enabled === false)
                continue;
            if (!wildcard(policy.tenant, identity.tenantId) ||
                !wildcard(policy.project, identity.projectId ?? "") ||
                !policyPattern(policy.action, action) ||
                !policyPattern(policy.resource, resource) ||
                !wildcard(policy.purpose, purpose)) {
                continue;
            }
            const subject = (policy.subject ?? "").trim();
            if (subject && subject !== "*" && subject !== identity.subject && subject !== identity.userId && subject !== identity.serviceIdentity)
                continue;
            const role = (policy.role ?? "").trim();
            if (role && role !== "*" && !(identity.roles ?? []).includes(role))
                continue;
            // The native data-plane query uses no caller-asserted attributes.
            if (Object.keys(policy.conditions ?? {}).length !== 0)
                continue;
            if (policy.relationship) {
                const related = this.tuples.some((tuple) => tuple.relation === policy.relationship &&
                    tuple.object === resource &&
                    (tuple.subject === identity.subject || tuple.subject === identity.userId || tuple.subject === identity.serviceIdentity) &&
                    wildcard(tuple.tenant, identity.tenantId) &&
                    wildcard(tuple.project, identity.projectId ?? ""));
                if (!related)
                    continue;
            }
            if (policy.effect === "deny")
                return false;
            if ((policy.required_scopes ?? []).every((scope) => hasScope(identity, scope)))
                allowed = true;
        }
        return allowed;
    }
    admit(method, identity, request) {
        const actual = method === "Authenticate" ? this.identities.get(request?.api_key) ?? identity : identity;
        for (const fault of this.faults) {
            if (fault.times === 0 || (fault.method && fault.method !== method) || (fault.tenantId && fault.tenantId !== actual.tenantId))
                continue;
            fault.times -= 1;
            let reason = fault.reason ?? "";
            if (!reason) {
                if (fault.code === codes.UNAVAILABLE)
                    reason = "UDB_BACKEND_UNAVAILABLE";
                else if (fault.code === codes.RESOURCE_EXHAUSTED)
                    reason = "UDB_RATE_LIMITED";
                else if (fault.code === codes.ABORTED)
                    reason = "UDB_CAS_CONFLICT";
            }
            throw (0, refusal_1.refuse)(fault.code, reason, `injected ${method} failure`, {
                reason,
                kind: (0, refusal_1.errorKind)(fault.code, reason),
                retryable: fault.code === codes.UNAVAILABLE || fault.code === codes.RESOURCE_EXHAUSTED,
                retry_after_ms: fault.retryAfterMs ?? 0,
            });
        }
        if (method === "Authenticate" || this.rate.limit === 0)
            return;
        let key = `${actual.tenantId}\0${method}`;
        if (this.rate.perPrincipal)
            key += `\0${actual.subject ?? ""}`;
        const now = Date.now();
        let bucket = this.rate.buckets.get(key);
        if (!bucket || now - bucket.start >= this.rate.windowMs) {
            bucket = { start: Math.floor(now / this.rate.windowMs) * this.rate.windowMs, count: 0 };
        }
        bucket.count += 1;
        this.rate.buckets.set(key, bucket);
        if (bucket.count <= this.rate.limit)
            return;
        throw (0, refusal_1.refuse)(codes.RESOURCE_EXHAUSTED, "UDB_RATE_LIMITED", "verified caller exceeded the configured rate limit", {
            reason: "UDB_RATE_LIMITED",
            kind: "ERROR_KIND_RATE_LIMITED",
            retryable: true,
            retry_after_ms: Math.max(1, this.rate.windowMs - (now - bucket.start)),
            operation: method,
            missing: { bucket: key, limit: String(this.rate.limit), window_ms: String(this.rate.windowMs), principal: actual.subject ?? "" },
        });
    }
    authenticate(key) {
        const identity = this.identities.get(key);
        if (!identity || !key || !identity.tenantId)
            throw (0, refusal_1.refuse)(codes.UNAUTHENTICATED, "", "unknown API key");
        const token = (0, node_crypto_1.randomBytes)(32).toString("base64url");
        const expiresAtUnix = Math.floor((Date.now() + this.tokenLifetimeMs) / 1000);
        this.tokens.set(token, { identity, expires: expiresAtUnix * 1000 });
        return {
            access_token: token,
            expires_at_unix: expiresAtUnix,
            principal: {
                tenant_id: identity.tenantId,
                project_id: identity.projectId ?? "",
                principal_id: identity.subject ?? "",
                subject: identity.subject ?? "",
                user_id: identity.userId ?? "",
                service_identity: identity.serviceIdentity ?? "",
                scopes: identity.scopes ?? [],
                roles: identity.roles ?? [],
                expires_at_unix: expiresAtUnix,
            },
        };
    }
    // ── Data verbs ────────────────────────────────────────────────────────────
    table(messageType, tenant) {
        let byTenant = this.rowsByType.get(messageType);
        if (!byTenant) {
            byTenant = new Map();
            this.rowsByType.set(messageType, byTenant);
        }
        let table = byTenant.get(tenant);
        if (!table) {
            table = new Map();
            byTenant.set(tenant, table);
        }
        return table;
    }
    /** The implicit column list and export-control gate: even a PII reader must name a
     *  PII column explicitly. */
    readColumns(entity, ctx, fields) {
        const selected = new Set();
        let canRead = hasScope(ctx.identity, "udb:pii:read");
        const purpose = ctx.metadata.get("x-purpose");
        if (purpose.length === 1 && ["export", "verification", "audit"].includes(String(purpose[0])))
            canRead = true;
        const guarded = (name) => {
            const field = entity.columns.get(name);
            return field.pii || field.encrypted || Boolean(field.column?.encrypted);
        };
        if (fields.length === 0) {
            for (const [name, field] of entity.columns) {
                if (field.pii || field.encrypted)
                    continue;
                if (guarded(name) && !canRead)
                    throw missingScopeRefusal("udb:pii:read");
                selected.add(name);
            }
            return selected;
        }
        for (const requested of fields) {
            const name = entity.aliases.get(requested.trim().toLowerCase());
            if (!name)
                throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", `unknown selected column ${requested}`);
            if (guarded(name) && !canRead)
                throw missingScopeRefusal("udb:pii:read");
            selected.add(name);
        }
        return selected;
    }
    select(ctx, req) {
        const entity = this.entity(req.message_type);
        const identity = ctx.identity;
        const filter = entity.queryFilter(struct(req.filter));
        entity.checkTenant(filter, identity.tenantId);
        const selected = this.readColumns(entity, ctx, req.fields ?? []);
        if (req.include_revision)
            for (const column of entity.primaryKey)
                selected.add(column);
        const found = [];
        for (const row of this.rowsByType.get(entity.messageType)?.get(identity.tenantId)?.values() ?? []) {
            if (entity.live(row) && entity.visibleTo(row, identity) && (0, filter_1.matches)(row, filter))
                found.push(row);
        }
        const order = [];
        for (const requested of req.sort ?? []) {
            const column = entity.aliases.get(String(requested.field).trim().toLowerCase());
            if (!column)
                throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", `unknown sort column ${requested.field}`);
            order.push({ field: column, descending: Boolean(requested.descending) });
        }
        for (const column of entity.primaryKey)
            order.push({ field: column, descending: false });
        found.sort((a, b) => {
            for (const key of order) {
                const c = (0, filter_1.compare)(a[key.field] ?? null, b[key.field] ?? null);
                if (c !== 0)
                    return c < 0 !== key.descending ? -1 : 1;
            }
            return 0;
        });
        const limit = req.limit > 0 ? req.limit : 100;
        let offset = 0;
        if (req.page_token) {
            offset = Number(Buffer.from(req.page_token, "base64url").toString("utf8"));
            if (!Number.isInteger(offset) || offset < 0 || offset > found.length) {
                throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "invalid page_token offset");
            }
        }
        const end = Math.min(offset + limit, found.length);
        const records = [];
        const revisions = [];
        const redacted = new Set();
        const canRead = hasScope(identity, "udb:pii:read");
        for (const row of found.slice(Math.min(offset, found.length), end)) {
            const view = {};
            for (const column of selected) {
                if (column in row)
                    view[column] = entity.wireValue(column, (0, entityRules_1.copyValue)(row[column]));
            }
            if (!canRead) {
                for (const [column, field] of entity.columns) {
                    if (field.masked && column in view) {
                        view[column] = "***MASKED***";
                        redacted.add(column);
                    }
                }
            }
            records.push(Buffer.from((0, recordCodec_1.recordToJson)(view), "utf8"));
            if (req.include_revision)
                revisions.push(this.revision(identity, entity, entity.keyOf(row)));
        }
        const hasMore = records.length >= limit;
        return {
            records_json: records,
            rows: records.map(() => ({})),
            record_revisions: revisions,
            redacted_fields: [...redacted].sort(),
            has_more: hasMore,
            next_page_token: hasMore && end < found.length ? Buffer.from(String(end)).toString("base64url") : "",
            exact_total: req.include_total ? found.length : 0,
        };
    }
    write(identity, req, insertOnly) {
        const tenant = identity.tenantId;
        if (insertOnly && ((req.conflict_fields ?? []).length > 0 || hasFields(req.expected))) {
            throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "Insert does not accept conflict_fields or expected");
        }
        const entity = this.entity(req.message_type);
        const record = entity.normalize(req.payload ? struct(req.payload) : decodeObject(req.record_json));
        entity.bindRecordScope(record, identity);
        entity.conflictFields(req.conflict_fields ?? []);
        entity.defaults(record);
        const key = entity.keyOf(record);
        const byTenant = this.rowsByType.get(entity.messageType);
        if (insertOnly) {
            for (const rows of byTenant?.values() ?? []) {
                if (rows.has(key))
                    throw (0, entityRules_1.uniqueRefusal)(`${entity.tableName}_pkey`);
            }
        }
        else {
            for (const [owner, rows] of byTenant ?? []) {
                const existing = rows.get(key);
                if (existing && (owner !== tenant || !entity.sameRecordScope(existing, record))) {
                    if (hasFields(req.expected)) {
                        throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "compare-and-swap target does not exist in the verified scope");
                    }
                    // The native ON CONFLICT scope guard leaves a foreign row untouched.
                    return { mutation_id: key, affected_rows: 0 };
                }
            }
        }
        const table = this.table(entity.messageType, tenant);
        const expected = entity.expectedColumns(struct(req.expected));
        if (Object.keys(expected).length > 0) {
            const current = table.get(key);
            if (!current)
                throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "compare-and-swap precondition failed: the target row does not exist");
            if (!(0, filter_1.holds)(current, expected))
                throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed: a field did not match the current row");
        }
        if (entity.softDelete && !insertOnly && !(entity.softDelete in record))
            record[entity.softDelete] = null;
        this.validateRow(entity, tenant, key, record);
        const current = table.get(key);
        if (current && !insertOnly) {
            // Isolation columns are excluded from the native conflict UPDATE SET.
            for (const column of [entity.tenantColumn, entity.projectColumn]) {
                if (column && column in current)
                    record[column] = (0, entityRules_1.copyValue)(current[column]);
            }
        }
        table.set(key, record);
        return { mutation_id: key, affected_rows: 1, revision: this.bumpRevision(identity, entity, key) };
    }
    validateRow(entity, tenant, key, row) {
        for (const [name, field] of entity.columns) {
            if ((field.column?.not_null || field.column?.primary_key) && (row[name] === null || row[name] === undefined)) {
                throw (0, entityRules_1.columnRefusal)(name);
            }
        }
        for (const [otherTenant, rows] of this.rowsByType.get(entity.messageType) ?? []) {
            for (const [otherKey, stored] of rows) {
                if (otherTenant === tenant && otherKey === key)
                    continue;
                if (otherKey === key)
                    throw (0, entityRules_1.uniqueRefusal)(`${entity.tableName}_pkey`);
                for (const unique of entity.unique) {
                    if (!entity.inUnique(unique, row) || !entity.inUnique(unique, stored))
                        continue;
                    // SQL UNIQUE treats NULLs as distinct.
                    const collision = unique.columns.every((name) => row[name] !== null && row[name] !== undefined && stored[name] !== null && stored[name] !== undefined && (0, filter_1.equal)(row[name], stored[name]));
                    if (collision)
                        throw (0, entityRules_1.uniqueRefusal)(unique.name);
                }
            }
        }
    }
    matchedKeys(entity, identity, table, filter) {
        const keys = [];
        for (const [key, row] of table) {
            if (entity.live(row) && entity.visibleTo(row, identity) && (0, filter_1.matches)(row, filter))
                keys.push(key);
        }
        return keys;
    }
    update(identity, req) {
        const tenant = identity.tenantId;
        const entity = this.entity(req.message_type);
        const filter = entity.queryFilter(struct(req.filter));
        if (Object.keys(filter).length === 0)
            throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "an update needs a filter");
        entity.checkTenant(filter, tenant);
        const rawChanges = struct(req.changes);
        const increments = req.increments ?? [];
        entity.updateColumns(rawChanges, increments);
        const expected = entity.expectedColumns(struct(req.expected));
        if (Object.keys(expected).length > 0)
            entity.validateCasKey(filter);
        const changes = entity.normalize(rawChanges);
        const table = this.table(entity.messageType, tenant);
        const keys = this.matchedKeys(entity, identity, table, filter);
        if (Object.keys(expected).length > 0) {
            if (keys.length === 0)
                throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "CAS row is missing");
            if (keys.length !== 1 || !(0, filter_1.holds)(table.get(keys[0]), expected)) {
                throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed");
            }
        }
        this.checkRevision(identity, entity, filter, req.expected_revision);
        if (req.require_affected > 0 && keys.length !== req.require_affected) {
            throw (0, refusal_1.refuse)(codes.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", "write matched a different number of rows than require_affected");
        }
        // Build the entire statement result before publishing it. A late unique or NOT
        // NULL violation rolls back every earlier matched row, like PostgreSQL.
        const candidate = new Map(table);
        for (const key of keys)
            candidate.delete(key);
        const changed = [];
        for (const key of keys) {
            const row = (0, entityRules_1.copyValue)(table.get(key));
            for (const [name, value] of Object.entries(changes))
                row[name] = (0, entityRules_1.copyValue)(value);
            for (const increment of increments) {
                const name = entity.aliases.get(increment.column.trim().toLowerCase());
                if (!name)
                    throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "unknown increment column");
                row[name] = entity.incrementValue(name, row[name] ?? null, increment.delta);
            }
            const newKey = entity.keyOf(row);
            if (candidate.has(newKey))
                throw (0, entityRules_1.uniqueRefusal)(`${entity.tableName}_pkey`);
            candidate.set(newKey, row);
            changed.push(newKey);
        }
        const byTenant = this.rowsByType.get(entity.messageType);
        byTenant.set(tenant, candidate);
        try {
            for (const key of changed)
                this.validateRow(entity, tenant, key, candidate.get(key));
        }
        catch (err) {
            byTenant.set(tenant, table);
            throw err;
        }
        const response = { affected_rows: keys.length };
        if (keys.length > 0) {
            try {
                response.revision = this.bumpRevision(identity, entity, this.revisionFilterKey(identity, entity, filter));
            }
            catch {
                // A filter that does not pin one row has no single revision to report.
            }
        }
        return response;
    }
    remove(identity, req) {
        const tenant = identity.tenantId;
        const entity = this.entity(req.message_type);
        const filter = entity.queryFilter(struct(req.filter));
        if (Object.keys(filter).length === 0)
            throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "a delete needs a filter");
        entity.checkTenant(filter, tenant);
        const expected = entity.expectedColumns(struct(req.expected));
        if (Object.keys(expected).length > 0)
            entity.validateCasKey(filter);
        const table = this.table(entity.messageType, tenant);
        const keys = this.matchedKeys(entity, identity, table, filter);
        if (Object.keys(expected).length > 0) {
            if (keys.length === 0)
                throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "CAS row is missing");
            if (keys.length !== 1 || !(0, filter_1.holds)(table.get(keys[0]), expected)) {
                throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed");
            }
        }
        this.checkRevision(identity, entity, filter, req.expected_revision);
        if (req.require_affected > 0 && keys.length !== req.require_affected) {
            throw (0, refusal_1.refuse)(codes.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", `the write matched ${keys.length} row(s) but require_affected is ${req.require_affected}; nothing was changed`);
        }
        for (const key of keys) {
            if (!entity.softDelete)
                table.delete(key);
            else
                table.set(key, { ...(0, entityRules_1.copyValue)(table.get(key)), [entity.softDelete]: new Date().toISOString() });
        }
        return { affected_rows: keys.length };
    }
    // ── Revisions and idempotency ─────────────────────────────────────────────
    revisionKey(identity, entity, key) {
        return [identity.tenantId, identity.projectId ?? "", entity.messageType, key].join("\0");
    }
    /** Entries outlive a delete: re-inserting a key must never revive a revision. */
    bumpRevision(identity, entity, key) {
        const scope = this.revisionKey(identity, entity, key);
        const next = (this.revisions.get(scope) ?? 0) + 1;
        this.revisions.set(scope, next);
        return String(next);
    }
    revision(identity, entity, key) {
        const current = this.revisions.get(this.revisionKey(identity, entity, key));
        return current ? String(current) : "";
    }
    /** Opaque revisions address only primary keys. */
    revisionFilterKey(identity, entity, filter) {
        const scoped = entity.scopedWriteFilter(filter, identity);
        const key = {};
        for (const column of entity.primaryKey) {
            let value = scoped[column];
            let present = column in scoped;
            if (typeof value === "object" && value !== null && !Array.isArray(value)) {
                present = "$eq" in value && Object.keys(value).length === 1;
                value = value.$eq;
            }
            if (!present || value === null || value === undefined) {
                throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_CAS_KEY_NOT_PK", "conditional mutation requires an equality filter on every primary-key column", {
                    reason: "UDB_CAS_KEY_NOT_PK",
                    kind: "ERROR_KIND_VALIDATION",
                    field_violations: [{ field: column, description: "primary-key column must be pinned by equality" }],
                });
            }
            key[column] = value;
        }
        return entity.keyOf(entity.normalize(key));
    }
    checkRevision(identity, entity, filter, expected) {
        const trimmed = (expected ?? "").trim();
        if (!trimmed)
            return;
        const key = this.revisionFilterKey(identity, entity, filter);
        if (!/^-?\d+$/.test(trimmed)) {
            throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "expected_revision is not a valid revision token", {
                kind: "ERROR_KIND_VALIDATION",
                field_violations: [{ field: "expected_revision", description: "must be an opaque revision token previously returned by the broker" }],
            });
        }
        if (this.revisions.get(this.revisionKey(identity, entity, key)) !== Number(trimmed)) {
            throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_REVISION_CONFLICT", "revision precondition failed: the row's current revision differs from expected_revision, or the row is not revision-tracked", {
                reason: "UDB_REVISION_CONFLICT",
                kind: "ERROR_KIND_CONFLICT",
                field_violations: [{ field: "expected_revision", description: "the current revision differs from the expected revision" }],
            });
        }
    }
    /** Only successful writes keep a receipt; it becomes visible with the row change.
     *  Replay precedes CAS and row matching, so a lost-response retry does not fail
     *  because the first write changed the row. */
    keyedWrite(ctx, operation, req, write) {
        const key = req.idempotency_key ?? "";
        if (!key)
            return write();
        if (/[\s\p{Cc}]/u.test(key)) {
            throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "idempotency_key must contain no whitespace or control characters", {
                kind: "ERROR_KIND_VALIDATION",
                field_violations: [{ field: "idempotency_key", description: "must contain no whitespace or control characters" }],
            });
        }
        const entity = this.entity(req.message_type);
        const identity = ctx.identity;
        const hash = (0, node_crypto_1.createHash)("sha256").update((0, recordCodec_1.recordToJson)(this.mutationInputs(entity, identity, operation, req))).digest("hex");
        const scope = [identity.tenantId, identity.projectId ?? "", req.message_type, operation, key].join("\0");
        const receipt = this.receipts.get(scope);
        if (receipt) {
            if (receipt.hash !== hash) {
                throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "UDB_IDEMPOTENCY_REUSE", "idempotency_key was already used for a different request", {
                    reason: "UDB_IDEMPOTENCY_REUSE",
                    kind: "ERROR_KIND_CONFLICT",
                    fix_hint: "Reuse a key only to retry identical inputs; use a new key for a different write.",
                    field_violations: [{ field: "idempotency_key", description: "already used with different inputs" }],
                });
            }
            return { ...receipt.response, was_duplicate: true };
        }
        const response = write();
        this.receipts.set(scope, { hash, response: { ...response } });
        return response;
    }
    mutationInputs(entity, identity, operation, req) {
        const inputs = { expected: hasFields(req.expected) ? struct(req.expected) : null };
        if (operation === "upsert" || operation === "insert") {
            const record = entity.normalize(req.payload ? struct(req.payload) : decodeObject(req.record_json));
            entity.bindRecordScope(record, identity);
            inputs.record = record;
            inputs.conflict_fields = req.conflict_fields ?? [];
            return inputs;
        }
        const revision = (req.expected_revision ?? "").trim();
        if (revision)
            inputs.expected_revision = revision;
        if (req.require_affected)
            inputs.require_affected = req.require_affected;
        if (operation === "update") {
            inputs.changes = entity.normalize(struct(req.changes));
            inputs.increments = (req.increments ?? []).map((increment) => [increment.column, increment.delta]);
        }
        inputs.filter = entity.scopedWriteFilter(entity.queryFilter(struct(req.filter)), identity);
        return inputs;
    }
    // ── Transactions ──────────────────────────────────────────────────────────
    /** Apply a transaction all-or-nothing: any refusal rolls every earlier mutation
     *  back. Its outbox events are recorded only when it commits. */
    beginTx(call) {
        let ctx;
        const fail = (err) => {
            if (!done) {
                done = true;
                call.emit("error", (0, refusal_1.statusOf)(err));
            }
        };
        let done = false;
        try {
            ctx = this.beforeRpc("BeginTx", call.metadata, null);
        }
        catch (err) {
            fail(err);
            return;
        }
        const mutations = [];
        let terminal;
        call.on("data", (m) => {
            if (done)
                return;
            try {
                if (terminal)
                    throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "mutation after transaction terminal marker");
                bindBody(ctx.identity, m.context);
                if (m.commit && m.rollback)
                    throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "commit and rollback cannot both be set");
                if (m.operation) {
                    const operation = String(m.operation).toLowerCase();
                    m.operation = operation;
                    let action = "";
                    let resource = m.message_type;
                    if (operation === "upsert" || operation === "insert")
                        action = "Upsert";
                    else if (operation === "update")
                        action = "Update";
                    else if (operation === "delete")
                        action = "Delete";
                    else if (operation === "enqueue_outbox_event") {
                        action = "EnqueueOutboxEvent";
                        resource = m.collection || m.message_type;
                    }
                    else
                        throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", `unsupported transaction operation ${operation}`);
                    if (String(m.idempotency_key ?? "").trim()) {
                        throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "this transaction path cannot honour idempotency_key; nothing was written", {
                            kind: "ERROR_KIND_VALIDATION",
                            field_violations: [{ field: `mutations[${mutations.length}].idempotency_key`, description: "not supported on this operation inside BeginTx; leave it empty" }],
                        });
                    }
                    this.authorize(ctx, resource, action);
                    mutations.push(m);
                }
                if (m.commit || m.rollback)
                    terminal = m;
            }
            catch (err) {
                fail(err);
            }
        });
        call.on("error", () => {
            done = true;
        });
        call.on("end", () => {
            if (done)
                return;
            try {
                if (!terminal)
                    throw (0, refusal_1.refuse)(codes.FAILED_PRECONDITION, "", "transaction closed without commit or rollback");
                if (terminal.rollback) {
                    call.write({ tx_id: terminal.tx_id, state: "TX_STATE_ROLLED_BACK" });
                }
                else {
                    const snapshot = this.snapshot();
                    const revisions = new Map(this.revisions);
                    const events = this.eventLog.length;
                    try {
                        this.applyTx(ctx.identity, mutations);
                    }
                    catch (err) {
                        this.rowsByType = snapshot;
                        this.revisions = revisions;
                        this.eventLog.length = events;
                        throw err;
                    }
                    call.write({ tx_id: terminal.tx_id, state: "TX_STATE_COMMITTED", message: `${mutations.length} mutation(s) committed` });
                }
                done = true;
                call.end();
            }
            catch (err) {
                fail(err);
            }
        });
    }
    applyTx(identity, mutations) {
        for (const m of mutations) {
            switch (m.operation) {
                case "upsert":
                case "insert":
                    this.write(identity, { message_type: m.message_type, record_json: m.record_json, payload: m.payload, expected: m.expected }, m.operation === "insert");
                    break;
                case "update":
                    this.update(identity, { message_type: m.message_type, filter: m.filter, changes: m.changes, expected: m.expected, increments: m.increments });
                    break;
                case "delete":
                    this.remove(identity, { message_type: m.message_type, filter: m.filter, expected: m.expected });
                    break;
                case "enqueue_outbox_event": {
                    const envelope = struct(m.payload);
                    if (envelope.document_id !== m.object_key || !m.object_key) {
                        throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "outbox partition_key must equal payload.document_id");
                    }
                    if (typeof envelope.tenant_id === "string" && envelope.tenant_id && envelope.tenant_id !== identity.tenantId) {
                        throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "outbox tenant differs from verified tenant");
                    }
                    if (typeof envelope.project_id === "string" && envelope.project_id && envelope.project_id !== (identity.projectId ?? "")) {
                        throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "outbox project differs from verified project");
                    }
                    if (envelope.event_id == null || envelope.event_type == null || envelope.envelope_version !== 2) {
                        throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "outbox needs a versioned event envelope");
                    }
                    envelope.tenant_id = identity.tenantId;
                    envelope.project_id = identity.projectId ?? "";
                    this.eventLog.push({ topic: m.collection || m.message_type, partitionKey: m.object_key, envelope: (0, entityRules_1.copyValue)(envelope) });
                    break;
                }
                default:
                    throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", `unsupported transaction operation ${m.operation}`);
            }
        }
    }
    snapshot() {
        const out = new Map();
        for (const [messageType, byTenant] of this.rowsByType) {
            const copied = new Map();
            for (const [tenant, table] of byTenant) {
                copied.set(tenant, new Map([...table].map(([key, row]) => [key, (0, entityRules_1.copyValue)(row)])));
            }
            out.set(messageType, copied);
        }
        return out;
    }
    // ── Durable consumers ─────────────────────────────────────────────────────
    /** Keeps a named consumer's verified owner, uses its stored position, and
     *  revalidates authorization and expiry throughout the stream. */
    publishCdc(call) {
        const fail = (err) => call.emit("error", (0, refusal_1.statusOf)(err));
        let ctx;
        let pattern;
        let next = 0;
        try {
            ctx = this.beforeRpc("PublishCDC", call.metadata, null);
            const req = call.request;
            bindBody(ctx.identity, req.context);
            pattern = topicPattern(req.topic_pattern);
            this.authorize(ctx, pattern, "PublishCDC");
            if (String(req.consumer_name ?? "").trim()) {
                const name = consumerName(req.consumer_name);
                const owner = consumerOwner(ctx.identity);
                const key = cursorKey(ctx.identity, name, pattern);
                let cursor = this.cursors.get(key);
                if (cursor && cursor.owner !== owner)
                    throw cursorOwnerError();
                if (!cursor) {
                    cursor = { owner, index: -1, id: "" };
                    this.cursors.set(key, cursor);
                }
                if (!req.since_event_id)
                    next = cursor.index + 1;
            }
            if (req.since_event_id) {
                const index = this.eventLog.findIndex((event) => event.envelope.event_id === req.since_event_id && eventInScope(event, ctx.identity, pattern));
                if (index < 0)
                    throw (0, refusal_1.refuse)(codes.NOT_FOUND, "", "CDC resume event is unknown or outside the verified scope");
                next = index + 1;
            }
        }
        catch (err) {
            fail(err);
            return;
        }
        let stopped = false;
        const pump = () => {
            if (stopped)
                return;
            try {
                if (Date.now() >= ctx.token.expires)
                    throw (0, refusal_1.refuse)(codes.UNAUTHENTICATED, "", "bearer expired during CDC stream");
                this.authorize(ctx, pattern, "PublishCDC");
                const pending = this.eventLog.slice(next);
                next = this.eventLog.length;
                for (const event of pending) {
                    if (!eventInScope(event, ctx.identity, pattern))
                        continue;
                    call.write({
                        event_id: String(event.envelope.event_id ?? ""),
                        topic: event.topic,
                        partition_key: event.partitionKey,
                        payload_json: (0, recordCodec_1.recordToJson)(event.envelope),
                    });
                }
            }
            catch (err) {
                stop();
                fail(err);
            }
        };
        const timer = setInterval(pump, 20);
        const stop = () => {
            stopped = true;
            clearInterval(timer);
        };
        call.on("cancelled", stop);
        call.on("close", stop);
        call.on("error", stop);
        pump();
    }
    nack(ctx, req) {
        if (req.attempts < 1 || req.attempts > 1000 || Buffer.byteLength(req.failure_reason ?? "") > 2048) {
            throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "invalid handler attempts or diagnostic length");
        }
        return this.settle(ctx, req, req);
    }
    settle(ctx, req, failure) {
        const name = consumerName(req.consumer_name ?? "");
        if (!/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(req.event_id ?? "")) {
            throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "event_id must be a UUID");
        }
        const pattern = topicPattern(req.topic_pattern);
        this.authorize(ctx, pattern, "PublishCDC");
        const identity = ctx.identity;
        const owner = consumerOwner(identity);
        const index = this.eventLog.findIndex((event) => event.envelope.event_id === req.event_id && eventInScope(event, identity, pattern));
        if (index < 0)
            throw (0, refusal_1.refuse)(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "acknowledged CDC event is not retained in the verified scope");
        const key = cursorKey(identity, name, pattern);
        const cursor = this.cursors.get(key) ?? { owner, index: -1, id: "" };
        if (cursor.owner !== owner)
            throw cursorOwnerError();
        if (failure) {
            const event = this.eventLog[index];
            let entry = this.dlq.get(req.event_id);
            const now = Math.floor(Date.now() / 1000);
            if (!entry) {
                entry = {
                    identity,
                    event,
                    failures: {},
                    failureMetadata: {
                        service: "udb-cdc-consumer",
                        error_type: "CONSUMER_HANDLER_FAILED",
                        error_message: failure.failure_reason,
                        failed_at: new Date().toISOString(),
                        retry_count: 0,
                    },
                    record: {
                        dlq_id: (0, node_crypto_1.randomUUID)(),
                        event_id: req.event_id,
                        topic: event.topic,
                        error_type: "CONSUMER_HANDLER_FAILED",
                        error_message: failure.failure_reason,
                        created_at_unix: now,
                    },
                };
            }
            const failureKey = (0, node_crypto_1.createHash)("sha256").update([owner, name, pattern].map((part) => `${part.length}:${part}`).join("")).digest("hex");
            if (!(failureKey in entry.failures)) {
                entry.failures = {
                    ...entry.failures,
                    [failureKey]: { owner_identity: owner, consumer_name: name, topic_pattern: pattern, attempts: failure.attempts, failure_reason: failure.failure_reason, failed_at: new Date().toISOString() },
                };
                entry.record.payload_json = Buffer.from((0, recordCodec_1.recordToJson)({ failed_event: event.envelope, failure_metadata: entry.failureMetadata, consumer_failures: entry.failures }));
                entry.record.status = "QUARANTINED";
                entry.record.updated_at_unix = now;
                this.dlq.set(req.event_id, entry);
            }
        }
        if (index > cursor.index) {
            cursor.index = index;
            cursor.id = req.event_id;
        }
        this.cursors.set(key, cursor);
        return { consumer_name: name, topic_pattern: pattern, event_id: req.event_id, acked_at_unix: Math.floor(Date.now() / 1000) };
    }
    dlqEntry(ctx, id) {
        for (const entry of this.dlq.values()) {
            if (entry.record.dlq_id === id && sameDomain(entry.identity, ctx.identity))
                return entry;
        }
        throw (0, refusal_1.refuse)(codes.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", "DLQ event not found in verified tenant/project");
    }
    listDlq(ctx, req) {
        const events = [...this.dlq.values()]
            .filter((entry) => sameDomain(entry.identity, ctx.identity) &&
            (!req.topic || req.topic === entry.record.topic) &&
            (!req.status_filter || req.status_filter === entry.record.status))
            .map((entry) => ({ ...entry.record }))
            .sort((a, b) => (a.dlq_id < b.dlq_id ? -1 : 1));
        let offset = 0;
        if (req.page_token) {
            offset = Number(req.page_token);
            if (!Number.isInteger(offset) || offset < 0 || offset > events.length)
                throw (0, refusal_1.refuse)(codes.INVALID_ARGUMENT, "", "invalid DLQ page token");
        }
        const limit = req.limit > 0 ? req.limit : 100;
        const end = Math.min(events.length, offset + limit);
        return { events: events.slice(offset, end), total_count: events.length, next_page_token: end < events.length ? String(end) : "" };
    }
    replayDlq(ctx, req) {
        const entry = this.dlqEntry(ctx, req.dlq_id);
        if (!["OPEN", "RETRYING", "QUARANTINED"].includes(entry.record.status)) {
            throw (0, refusal_1.refuse)(codes.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", "DLQ event is not replayable");
        }
        const event = { topic: entry.event.topic, partitionKey: entry.event.partitionKey, envelope: (0, entityRules_1.copyValue)(entry.event.envelope) };
        entry.record.status = "REPLAYED";
        entry.record.updated_at_unix = Math.floor(Date.now() / 1000);
        // The canonical outbox's event id is unique: preserving it adds no duplicate
        // retained event to the journal.
        if (!req.preserve_event_id) {
            event.envelope.event_id = (0, node_crypto_1.randomUUID)();
            this.eventLog.push(event);
        }
        return { mutation_id: String(event.envelope.event_id), affected_rows: 1 };
    }
    setDlqStatus(ctx, req, status) {
        const entry = this.dlqEntry(ctx, req.dlq_id);
        entry.record.status = status;
        entry.record.updated_at_unix = Math.floor(Date.now() / 1000);
        return { mutation_id: req.dlq_id, affected_rows: 1 };
    }
}
exports.Fake = Fake;
/** Start a fake serving the given entity messages, with an allow-all test policy. It
 *  stops when `t` ends. Their annotations define keys and the tenant column. */
async function createFake(t, messageTypes, opts = {}) {
    const root = node_path_1.default.resolve(opts.protoRoot ?? (0, protoRoot_1.defaultProtoRoot)());
    const fake = new Fake((0, protoSchema_1.protoSchemas)(opts.protoRoot), root);
    fake.configure([], true);
    for (const messageType of messageTypes)
        fake.register(messageType);
    await fake.serve();
    t.after(() => fake.close());
    return fake;
}
/** Start a fake serving every table declared by a compiled descriptor set (the bytes
 *  of `protoc --include_imports --descriptor_set_out`, which must carry every import)
 *  or, without one, by a proto tree, under the given policies. With no policies it
 *  denies data requests by default, as a broker does. `policyFile` is the JSON or YAML
 *  file `udb policy apply` reads. Call `close()` when finished. */
async function createDescriptorFake(opts = {}) {
    if (opts.descriptorSet && opts.protoRoot)
        throw new Error("udbtest: pass descriptorSet or protoRoot, not both");
    const schemas = opts.descriptorSet ? protoSchema_1.ProtoSchemas.fromDescriptorSet(opts.descriptorSet) : (0, protoSchema_1.protoSchemas)(opts.protoRoot);
    const root = node_path_1.default.resolve((opts.descriptorSet ? undefined : opts.protoRoot) ?? (0, protoRoot_1.defaultProtoRoot)());
    const fake = new Fake(schemas, root);
    fake.configure(opts.policies ?? (opts.policyFile ? (0, policy_1.loadPolicies)(opts.policyFile) : []), false);
    const tables = schemas.tables();
    if (tables.length === 0)
        throw new Error("udbtest: descriptor set declares no tables");
    for (const table of tables)
        fake.register(table.fullName);
    await fake.serve();
    return fake;
}
