"use strict";
// Small facades over native services that every service otherwise wrapped by hand:
// evaluate feature flags, take a distributed lock, validate a user's session, and
// call as an end user.
Object.defineProperty(exports, "__esModule", { value: true });
exports.LockFacade = exports.FlagsFacade = void 0;
exports.validateSessionOn = validateSessionOn;
exports.asUserCall = asUserCall;
const stores_1 = require("./stores");
const CONFIG_SERVICE = "udb.core.config.services.v1.ConfigService";
const LOCK_SERVICE = "udb.core.lock.services.v1.LockService";
const AUTHN_SERVICE = "udb.core.authn.services.v1.AuthnService";
/** Evaluates feature flags for the connected tenant and project. */
class FlagsFacade {
    core;
    meta;
    constructor(core, meta) {
        this.core = core;
        this.meta = meta;
    }
    /** Resolve `keys` with `attrs` (for example `{ user_id: "..." }`). Unknown keys
     *  are absent from the result. */
    async evaluate(keys, attrs = {}, call) {
        const response = await this.core.unary(CONFIG_SERVICE, "EvaluateFlags", {
            tenant_id: this.meta.tenantId,
            keys,
            context: { project_id: this.meta.projectId ?? "", attributes: attrs },
        }, call);
        return response?.values ?? {};
    }
    /** Whether boolean flag `key` is on for `attrs`. A flag that does not exist is off. */
    async enabled(key, attrs = {}, call) {
        const values = await this.evaluate([key], attrs, call);
        return values[key]?.bool_value === true;
    }
}
exports.FlagsFacade = FlagsFacade;
/** Distributed locks on the control-plane channel. Each call defaults the request
 *  tenant to the connected tenant when empty and never overrides an explicit one. */
class LockFacade {
    core;
    meta;
    constructor(core, meta) {
        this.core = core;
        this.meta = meta;
    }
    invoke(method, request, call) {
        return this.core.unary(LOCK_SERVICE, method, { ...request, tenant_id: request.tenant_id || this.meta.tenantId }, call);
    }
    /** Acquire a lock. The reply's fencing token is presented to `renew` and `release`. */
    acquire(request, call) {
        return this.invoke("AcquireLock", request, call);
    }
    /** Extend a held lock's lease; a stale fencing token is refused. */
    renew(request, call) {
        return this.invoke("RenewLock", request, call);
    }
    /** Release a held lock; a stale fencing token is refused. */
    release(request, call) {
        return this.invoke("ReleaseLock", request, call);
    }
    /** Fetch one lock by name (`found: false` on a miss, not an error). */
    get(request, call) {
        return this.invoke("GetLock", request, call);
    }
    /** List the tenant's locks. */
    list(request = {}, call) {
        return this.invoke("ListLocks", request, call);
    }
}
exports.LockFacade = LockFacade;
function stripBearer(token) {
    return (token ?? "").trim().replace(/^Bearer /, "").trim();
}
function timestampToDate(value) {
    if (!value)
        return null;
    return new Date(Number(value.seconds ?? 0) * 1000 + Math.floor(Number(value.nanos ?? 0) / 1e6));
}
/** Check a user's bearer token (with or without the "Bearer " prefix) on `core`.
 *  An invalid or expired token is an error. */
async function validateSessionOn(core, token, call) {
    const response = await core.unary(AUTHN_SERVICE, "ValidateToken", { token: stripBearer(token), token_type: "TOKEN_TYPE_JWT_ACCESS" }, call);
    if (!response?.valid)
        throw new stores_1.NotFoundError("udb: not found: the session token is not valid");
    return {
        publicId: response.session_public_id ?? "",
        userId: response.user_id ?? "",
        tenantId: response.tenant_id ?? "",
        projectId: response.project_id ?? "",
        scopes: response.scopes ?? [],
        roles: response.roles ?? [],
        expiresAt: timestampToDate(response.expires_at),
        principal: response.principal ?? null,
    };
}
/** Call options whose call carries the end user's bearer instead of the service's
 *  own credential, for operations the broker must authorize as that user. The
 *  token may carry the "Bearer " prefix. The broker verifies it; no claim is adopted
 *  and the connected tenant and project do not change, so the user must belong to
 *  them. The service's principal hints are blanked, and exactly one credential is
 *  carried: a raw key in `call` does not ride along. Audit values in `call` stay. */
function asUserCall(bearer, call = {}) {
    const headers = { ...(call.headers ?? {}) };
    for (const name of Object.keys(headers)) {
        const lower = name.toLowerCase();
        if (lower === "x-api-key" || lower === "x-udb-api-key")
            delete headers[name];
    }
    const meta = { ...(call.meta ?? {}) };
    delete meta.apiKey;
    delete meta.bearerToken;
    return {
        ...call,
        meta,
        headers: {
            ...headers,
            authorization: `Bearer ${stripBearer(bearer)}`,
            "x-user-id": "",
            "x-service-identity": "",
            "x-scopes": "",
        },
    };
}
