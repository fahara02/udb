"use strict";
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
exports.AuthzCache = exports.UdbAuthClient = exports.UdbPolicyBundleError = exports.UdbAuthzDenied = void 0;
exports.verifyPolicyBundle = verifyPolicyBundle;
exports.withNativeTx = withNativeTx;
// Hand-written auth ergonomics over the generated AuthnService / AuthzService,
// mirroring `client.ts`'s dynamic proto-loader + metadata convention (item 108).
//
// Loads the native auth service protos from the same proto root the broker
// client uses; `google/api` annotation imports resolve from the vendored
// `third_party/googleapis` include dir.
const grpc = __importStar(require("@grpc/grpc-js"));
const protoLoader = __importStar(require("@grpc/proto-loader"));
const crypto = __importStar(require("crypto"));
const path_1 = __importDefault(require("path"));
const client_1 = require("./client");
const protoRoot_1 = require("./protoRoot");
function buildCredentials(opts) {
    if (opts.tls) {
        return grpc.credentials.createSsl(opts.tls.rootCerts, opts.tls.privateKey, opts.tls.certChain);
    }
    if (opts.secure) {
        return grpc.credentials.createSsl();
    }
    return grpc.credentials.createInsecure();
}
function loadAuth(target, protoRoot, opts = {}) {
    const includeDirs = [protoRoot, path_1.default.resolve(protoRoot, "../third_party/googleapis")];
    const loaderOptions = {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
        includeDirs,
    };
    const authnDef = protoLoader.loadSync("udb/core/authn/services/v1/authn_service.proto", loaderOptions);
    const authzDef = protoLoader.loadSync("udb/core/authz/services/v1/authz_service.proto", loaderOptions);
    const authn = grpc.loadPackageDefinition(authnDef);
    const authz = grpc.loadPackageDefinition(authzDef);
    const creds = buildCredentials(opts);
    const channelOptions = { ...client_1.UDB_DEFAULT_CHANNEL_OPTIONS, ...(opts.channelOptions ?? {}) };
    return {
        authn: new authn.udb.core.authn.services.v1.AuthnService(target, creds, channelOptions),
        authz: new authz.udb.core.authz.services.v1.AuthzService(target, creds, channelOptions),
    };
}
/** Thrown by `require()` when the bound principal is NOT allowed to perform the
 *  action. Carries the full server `Decision` so callers can inspect the
 *  `decision_id`, `deny_reason`, and `required_scopes`. */
class UdbAuthzDenied extends Error {
    resource;
    action;
    purpose;
    /** The server `Decision` message (decision_id / allowed / deny_reason / …). */
    decision;
    constructor(resource, action, purpose, decision) {
        const reason = decision?.deny_reason || "not allowed";
        const id = decision?.decision_id ? ` [decision ${decision.decision_id}]` : "";
        super(`udb: authorization denied for action '${action}': ${reason}${id}`);
        this.name = "UdbAuthzDenied";
        this.resource = resource;
        this.action = action;
        this.purpose = purpose;
        this.decision = decision;
    }
}
exports.UdbAuthzDenied = UdbAuthzDenied;
/** Thrown by {@link verifyPolicyBundle} / {@link UdbAuthClient.getPolicyBundle}
 *  when a signed policy bundle fails HMAC verification. */
class UdbPolicyBundleError extends Error {
    constructor(message) {
        super(`udb: policy bundle verification failed: ${message}`);
        this.name = "UdbPolicyBundleError";
    }
}
exports.UdbPolicyBundleError = UdbPolicyBundleError;
function bundleBytes(bundle) {
    if (typeof bundle === "string")
        return Buffer.from(bundle, "utf8");
    return Buffer.isBuffer(bundle) ? bundle : Buffer.from(bundle);
}
/** Throw a loud, typed error when a gated conformance-proof field is empty
 *  (broker not in conformance mode). */
function requireProof(value, field) {
    const s = typeof value === "string" ? value : "";
    if (!s) {
        throw new Error(`udb: conformanceProof: empty ${field} — broker is not in conformance/dev-echo mode`);
    }
    return s;
}
/** Decode an RFC-4648 base32 secret (TOTP `totp_secret`) to bytes. Tolerates
 *  lowercase + padding. */
function base32Decode(secret) {
    const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    const clean = secret.replace(/=+$/, "").toUpperCase().replace(/\s+/g, "");
    let bits = 0;
    let value = 0;
    const out = [];
    for (const ch of clean) {
        const idx = alphabet.indexOf(ch);
        if (idx === -1)
            continue;
        value = (value << 5) | idx;
        bits += 5;
        if (bits >= 8) {
            bits -= 8;
            out.push((value >>> bits) & 0xff);
        }
    }
    return Buffer.from(out);
}
/** Compute the current 6-digit RFC-6238 TOTP code (SHA-1, 30s step) from a
 *  base32 `totp_secret`. Stdlib `crypto` only — no new dependency. */
function totpNow(secret, stepSeconds = 30, digits = 6) {
    const key = base32Decode(secret);
    const counter = Math.floor(Date.now() / 1000 / stepSeconds);
    const buf = Buffer.alloc(8);
    // 64-bit big-endian counter (high 32 bits are ~0 for current epochs).
    buf.writeUInt32BE(Math.floor(counter / 0x100000000), 0);
    buf.writeUInt32BE(counter >>> 0, 4);
    const hmac = crypto.createHmac("sha1", key).update(buf).digest();
    const offset = hmac[hmac.length - 1] & 0x0f;
    const code = ((hmac[offset] & 0x7f) << 24) |
        ((hmac[offset + 1] & 0xff) << 16) |
        ((hmac[offset + 2] & 0xff) << 8) |
        (hmac[offset + 3] & 0xff);
    return (code % Math.pow(10, digits)).toString().padStart(digits, "0");
}
/**
 * Recompute the HMAC-SHA256 (lowercase hex) of `signed.bundle` keyed by `secret`
 * and compare it constant-time against `signed.signature`. Returns `true` on a
 * match, `false` otherwise (including a missing/empty signature). Never throws on
 * a mismatch — callers that want a throw should use {@link UdbAuthClient.getPolicyBundle}
 * with a configured `policyBundleSecret`.
 *
 * The server emits the signature as lowercase hex (the proto comment that says
 * Base64 is wrong); this verifier matches the server.
 */
function verifyPolicyBundle(signed, secret) {
    if (!signed || !secret)
        return false;
    const provided = signed.signature ?? "";
    const expected = crypto
        .createHmac("sha256", secret)
        .update(bundleBytes(signed.bundle))
        .digest("hex");
    // Constant-time compare; the lengths must match for timingSafeEqual.
    const a = Buffer.from(expected, "utf8");
    const b = Buffer.from(provided, "utf8");
    if (a.length !== b.length)
        return false;
    return crypto.timingSafeEqual(a, b);
}
/** Convenience wrapper over AuthnService + AuthzService. The same `UdbMetadata`
 *  used for broker calls is attached to every auth RPC. */
class UdbAuthClient {
    authn;
    authz;
    meta;
    policyBundleSecret;
    constructor(target, meta, protoRoot = (0, protoRoot_1.defaultProtoRoot)(), policyBundleSecret, options = {}) {
        const clients = loadAuth(target, protoRoot, options);
        this.authn = clients.authn;
        this.authz = clients.authz;
        this.meta = meta;
        this.policyBundleSecret = policyBundleSecret;
    }
    call(client, method, request) {
        return new Promise((resolve, reject) => {
            client[method](request, (0, client_1.metadata)(this.meta), (err, resp) => {
                if (err)
                    reject(err);
                else
                    resolve(resp);
            });
        });
    }
    setCredentials(credentials) {
        if ("bearerToken" in credentials) {
            this.meta.bearerToken = credentials.bearerToken;
        }
        if ("apiKey" in credentials) {
            this.meta.apiKey = credentials.apiKey;
        }
    }
    // ── Authentication ──────────────────────────────────────────────────────
    authenticate(request) {
        return this.call(this.authn, "Authenticate", request);
    }
    authenticateBearer(token) {
        return this.authenticate({
            bearer_token: token,
            tenant_hint: this.meta.tenantId,
            project_hint: this.meta.projectId ?? "",
            requested_scopes: this.meta.scopes ?? [],
        });
    }
    authenticateApiKey(apiKey) {
        return this.authenticate({
            api_key: apiKey,
            tenant_hint: this.meta.tenantId,
            project_hint: this.meta.projectId ?? "",
            requested_scopes: this.meta.scopes ?? [],
        });
    }
    authenticateSession(sessionId) {
        return this.authenticate({
            session_id: sessionId,
            tenant_hint: this.meta.tenantId,
            project_hint: this.meta.projectId ?? "",
        });
    }
    login(request) {
        return this.call(this.authn, "Login", request);
    }
    /** Canonical login accessor (naming contract): start an authenticated session
     *  via `AuthnService.Login`. Thin alias of {@link login} — EXACTLY one Login
     *  RPC, no hidden round trip. For automatic token-store persistence + refresh,
     *  use `UdbProject.login` / `UdbProject.loginAndAdoptTenant`. */
    loginSession(creds) {
        return this.login(creds);
    }
    refreshToken(request) {
        return this.call(this.authn, "RefreshToken", request);
    }
    /** Better Auth bridge: forward a Better Auth session/JWT (already verified by
     *  the app) to UDB as an external identity. UDB maps the verified claims to a
     *  principal; UDB policy — not Better Auth roles — still decides authorization. */
    authenticateBetterAuth(token, providerId = "better-auth") {
        return this.authenticate({
            external_provider_id: providerId,
            external_token: token,
            tenant_hint: this.meta.tenantId,
            project_hint: this.meta.projectId ?? "",
        });
    }
    // ── Conformance proof (dev/test only) ─────────────────────────────────────
    /**
     * Surface the gated proof material a HEADLESS conformance harness needs — it
     * does NOT bypass verification, it only reads what the broker's prod-guarded
     * dev-echo gate already exposes:
     *   - "otp"            → `SendOTP(...).dev_otp_code`
     *   - "password_reset" → `ForgotPassword(...).dev_otp_code`
     *   - "phone"          → `SendPhoneVerification(...).dev_otp_code`
     *   - "totp"           → compute the current RFC-6238 code from
     *                        `EnrollMFA(...).totp_secret` (no broker echo for TOTP)
     *
     * Rejects when the proof field is empty (broker NOT in conformance mode) so
     * callers fail loud, not silent. `args` carries the per-kind request fields
     * (e.g. `{ user_id }` / `{ identifier }` / `{ user_id, phone }`).
     */
    async conformanceProof(kind, args = {}) {
        const ctx = { context: args.context, ...args };
        switch (kind) {
            case "otp": {
                const resp = await this.call(this.authn, "SendOTP", ctx);
                return requireProof(resp?.dev_otp_code, "SendOTP.dev_otp_code");
            }
            case "password_reset": {
                const resp = await this.call(this.authn, "ForgotPassword", ctx);
                return requireProof(resp?.dev_otp_code, "ForgotPassword.dev_otp_code");
            }
            case "phone": {
                const resp = await this.call(this.authn, "SendPhoneVerification", ctx);
                return requireProof(resp?.dev_otp_code, "SendPhoneVerification.dev_otp_code");
            }
            case "totp": {
                const resp = await this.call(this.authn, "EnrollMFA", ctx);
                const secret = resp?.totp_secret ?? "";
                if (!secret) {
                    throw new Error("udb: conformanceProof(totp): EnrollMFA returned no totp_secret");
                }
                return totpNow(secret);
            }
        }
    }
    /** Explicit WebAuthn passkey flows. Each sequences Start → Finish over the
     *  generated ceremony RPCs, threading the single-use Start challenge into
     *  Finish — exactly two RPCs per flow, no hidden Get. */
    passkeys = {
        /** Register a passkey: StartWebAuthnRegistration → FinishWebAuthnRegistration.
         *  `credentialJson` is the authenticator's credential (or the dev soft-auth
         *  `TEST_CREDENTIAL_SENTINEL` when driving the test authenticator). */
        register: async (args, credentialJson) => {
            const start = await this.call(this.authn, "StartWebAuthnRegistration", {
                user_id: args.user_id,
                label: args.label ?? "",
                tenant_id: args.tenant_id ?? this.meta.tenantId,
                project_id: args.project_id ?? this.meta.projectId ?? "",
                context: args.context,
            });
            return this.call(this.authn, "FinishWebAuthnRegistration", {
                challenge_id: start?.challenge_id ?? "",
                public_key_credential_json: credentialJson,
                label: args.label ?? "",
                context: args.context,
            });
        },
        /** Authenticate with a passkey: StartWebAuthnAuthentication →
         *  FinishWebAuthnAuthentication. */
        authenticate: async (args, credentialJson) => {
            const start = await this.call(this.authn, "StartWebAuthnAuthentication", {
                user_id: args.user_id ?? this.meta.userId ?? "",
                tenant_id: args.tenant_id ?? this.meta.tenantId,
                project_id: args.project_id ?? this.meta.projectId ?? "",
                context: args.context,
            });
            return this.call(this.authn, "FinishWebAuthnAuthentication", {
                challenge_id: start?.challenge_id ?? "",
                public_key_credential_json: credentialJson,
                context: args.context,
            });
        },
    };
    // ── Authorization ─────────────────────────────────────────────────────────
    async authorize(request) {
        const resp = await this.call(this.authz, "Authorize", request);
        return resp.decision;
    }
    /** Raw `AuthzService.CreatePolicyRule` (one RPC, no hidden List/Get). Tenant
     *  defaults to the bound metadata when omitted. */
    createPolicyRule(request) {
        return this.call(this.authz, "CreatePolicyRule", {
            tenant_id: this.meta.tenantId,
            project_id: this.meta.projectId ?? "",
            ...request,
        });
    }
    /** Raw `AuthzService.AssignRole` (one RPC, no hidden List/Get). Tenant/domain
     *  default to the bound metadata when omitted. */
    assignRole(request) {
        return this.call(this.authz, "AssignRole", {
            tenant_id: this.meta.tenantId,
            project_id: this.meta.projectId ?? "",
            domain: this.meta.tenantId,
            ...request,
        });
    }
    /** Returns `[allowed, decision]` for the bound principal acting on a resource. */
    async can(resource, action, purpose = "") {
        const decision = await this.authorize({
            principal: {
                user_id: this.meta.userId ?? "",
                service_identity: this.meta.serviceIdentity ?? "",
                tenant_id: this.meta.tenantId,
                project_id: this.meta.projectId ?? "",
                scopes: this.meta.scopes ?? [],
            },
            tenant_id: this.meta.tenantId,
            project_id: this.meta.projectId ?? "",
            resource,
            action,
            purpose: purpose || this.meta.purpose,
            requested_scopes: this.meta.scopes ?? [],
        });
        return [Boolean(decision?.allowed), decision];
    }
    /** Like `can`, but throws {@link UdbAuthzDenied} (carrying the full decision)
     *  when the principal is not allowed. Returns the allowing `Decision`. */
    async require(resource, action, purpose = "") {
        const p = purpose || this.meta.purpose;
        const [allowed, decision] = await this.can(resource, action, p);
        if (!allowed)
            throw new UdbAuthzDenied(resource, action, p, decision);
        return decision;
    }
    /** Returns the full `Decision` (decision_id, allowed, deny_reason, …) without
     *  throwing, for callers that want to inspect the verdict rather than branch
     *  on a boolean. */
    async explain(resource, action, purpose = "") {
        const [, decision] = await this.can(resource, action, purpose || this.meta.purpose);
        return decision;
    }
    /** Check many (object, action) pairs in a single round-trip via the authz
     *  `BatchCheckPermissions` RPC. Returns the server's `results` map keyed by
     *  `"object:action"` → allowed, plus a `lookup(object, action)` helper. */
    async batchCan(checks) {
        const resp = await this.call(this.authz, "BatchCheckPermissions", {
            user_id: this.meta.userId ?? "",
            domain: this.meta.tenantId,
            checks: checks.map((c) => ({ object: c.object, action: c.action })),
            context: {},
        });
        const results = resp?.results ?? {};
        return {
            results,
            lookup: (object, action) => Boolean(results[`${object}:${action}`]),
        };
    }
    // ── Stage 2: native database fast-path access (item 138) ──────────────────
    /** Authorize and, when allowed, return the native-access grant (restricted
     *  role + scoped DSN + RLS session variables). Resolves to `null` when access
     *  is allowed but no grant was minted; rejects when the decision denied. */
    async nativeAccess(resource, action, purpose = "") {
        const resp = await this.call(this.authz, "GetNativeAccess", {
            principal: {
                user_id: this.meta.userId ?? "",
                service_identity: this.meta.serviceIdentity ?? "",
                tenant_id: this.meta.tenantId,
                project_id: this.meta.projectId ?? "",
                scopes: this.meta.scopes ?? [],
            },
            tenant_id: this.meta.tenantId,
            project_id: this.meta.projectId ?? "",
            resource,
            action,
            purpose: purpose || this.meta.purpose,
            requested_scopes: this.meta.scopes ?? [],
        });
        if (resp?.decision && !resp.decision.allowed) {
            throw new Error(`udb: native access denied: ${resp.decision.deny_reason ?? ""}`);
        }
        return resp?.grant ?? null;
    }
    // ── Stage 2: signed policy bundle (item 140) ──────────────────────────────
    /** Fetch the server's `SignedPolicyBundle`. When a `policyBundleSecret` is
     *  configured the bundle's HMAC-SHA256 (lowercase hex) signature is verified
     *  before the bundle is returned, and {@link UdbPolicyBundleError} is thrown
     *  on mismatch. With no secret configured the bundle is returned unverified. */
    async getPolicyBundle() {
        const resp = await this.call(this.authz, "GetPolicyBundle", {
            tenant_id: this.meta.tenantId,
            project_id: this.meta.projectId ?? "",
        });
        const bundle = resp?.bundle;
        if (this.policyBundleSecret && bundle) {
            if (!verifyPolicyBundle(bundle, this.policyBundleSecret)) {
                throw new UdbPolicyBundleError(`signature mismatch${bundle.key_id ? ` (key ${bundle.key_id})` : ""}`);
            }
        }
        return bundle;
    }
}
exports.UdbAuthClient = UdbAuthClient;
/** Apply a native-access grant's `app.current_*` session variables and run a
 *  transaction. `client` is any node-postgres-style client exposing `query`;
 *  the SDK pulls in no driver of its own. Commits on success, rolls back on
 *  error so RLS context never leaks across requests. */
async function withNativeTx(client, grant, fn) {
    await client.query("BEGIN");
    try {
        const vars = grant?.session_variables ?? {};
        for (const [key, value] of Object.entries(vars)) {
            await client.query("SELECT set_config($1, $2, true)", [key, value]);
        }
        const result = await fn();
        await client.query("COMMIT");
        return result;
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
}
/** Local authorization cache over `UdbAuthClient.can`, keyed by
 *  (principal, resource, action, purpose) and bounded by the server's
 *  `Decision.cache_ttl_seconds` (a zero TTL is never cached). */
class AuthzCache {
    client;
    now;
    cache = new Map();
    constructor(client, now = () => Date.now()) {
        this.client = client;
        this.now = now;
    }
    static key(meta, resource, action, purpose) {
        const subject = meta.userId || meta.serviceIdentity || "";
        const res = resource?.message_type || resource?.resource_name || resource?.table || "";
        return [meta.tenantId, meta.projectId ?? "", subject, res, action, purpose].join("");
    }
    async can(resource, action, purpose = "") {
        const meta = this.client.meta;
        const p = purpose || meta.purpose;
        const key = AuthzCache.key(meta, resource, action, p);
        const hit = this.cache.get(key);
        if (hit && this.now() < hit.expires) {
            return [Boolean(hit.decision?.allowed), hit.decision];
        }
        const [allowed, decision] = await this.client.can(resource, action, p);
        const ttl = Number(decision?.cache_ttl_seconds ?? 0);
        if (ttl > 0) {
            this.cache.set(key, { expires: this.now() + ttl * 1000, decision });
        }
        return [allowed, decision];
    }
    /** Cache-routed {@link UdbAuthClient.require}: throws {@link UdbAuthzDenied}
     *  on a denied (cached or fresh) decision; returns the allowing decision. */
    async require(resource, action, purpose = "") {
        const meta = this.client.meta;
        const p = purpose || meta.purpose;
        const [allowed, decision] = await this.can(resource, action, p);
        if (!allowed)
            throw new UdbAuthzDenied(resource, action, p, decision);
        return decision;
    }
    /** Cache-routed {@link UdbAuthClient.explain}: returns the full decision
     *  (cached or fresh) without throwing. */
    async explain(resource, action, purpose = "") {
        const [, decision] = await this.can(resource, action, purpose);
        return decision;
    }
    invalidate() {
        this.cache.clear();
    }
}
exports.AuthzCache = AuthzCache;
