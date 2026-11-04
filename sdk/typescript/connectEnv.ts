// The one environment contract a service connects with, and the assertions it
// makes about the principal it connected as. With prefix "UDB_" (the default):
//
//   UDB_TARGET            broker gRPC target, host:port               (required)
//   UDB_AUTH_TARGET       control-plane target; defaults to UDB_TARGET
//   UDB_WEBRTC_TARGET     WebRTC signalling target; defaults to the auth target
//   UDB_TENANT_ID         the service's tenant UUID                    (required)
//   UDB_PROJECT_ID        project (UDB_PROJECT is accepted too)
//   UDB_PURPOSE           purpose sent on every call                   (required)
//   UDB_API_KEY           service-account key, exchanged for a bearer  (required unless UDB_BEARER)
//   UDB_BEARER            a ready bearer token instead of an API key
//   UDB_SCOPES            requested scopes, comma or space separated
//   UDB_SERVICE_IDENTITY  the service identity the grant is bound to
//   UDB_DEADLINE          per-call deadline when the caller sets none (Go duration, e.g. 10s)
//   UDB_TLS               "true" to dial with TLS (system roots)
//   UDB_TLS_SERVER_NAME   TLS server name override

import * as grpc from "@grpc/grpc-js";

import type { UdbProjectConfig } from "./project";

const DURATION_UNITS_MS: Record<string, number> = {
  ns: 1e-6,
  us: 1e-3,
  "µs": 1e-3,
  "μs": 1e-3,
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
};

/** Milliseconds for a Go-style duration ("10s", "1m30s", "250ms"); undefined when
 *  `raw` is not one. Every SDK reads the same UDB_DEADLINE value. */
export function parseDurationMs(raw: string): number | undefined {
  const match = /^([+-])?(.*)$/s.exec(raw);
  const body = match?.[2] ?? "";
  if (body === "0") return 0;
  if (!body) return undefined;
  let total = 0;
  let rest = body;
  while (rest) {
    const part = /^(\d+\.?\d*|\.\d+)(ns|us|µs|μs|ms|s|m|h)/.exec(rest);
    if (!part) return undefined;
    total += Number(part[1]) * DURATION_UNITS_MS[part[2]];
    rest = rest.slice(part[0].length);
  }
  return match?.[1] === "-" ? -total : total;
}

function splitEnvScopes(raw: string): string[] | undefined {
  const scopes = [...new Set(raw.split(/[, \t]+/).filter(Boolean))];
  return scopes.length ? scopes : undefined;
}

/** Read the environment contract above. Every missing or invalid variable is
 *  reported in one error, so a misconfigured deployment is fixed in one pass. */
export function configFromEnv(prefix = "", env: NodeJS.ProcessEnv = process.env): UdbProjectConfig {
  const pre = prefix || "UDB_";
  const get = (name: string) => (env[pre + name] ?? "").trim();
  const problems: string[] = [];
  const need = (name: string) => {
    const value = get(name);
    if (!value) problems.push(`${pre}${name} is not set`);
    return value;
  };

  const target = need("TARGET");
  const authTarget = get("AUTH_TARGET");
  const webrtcTarget = get("WEBRTC_TARGET");
  const tenantId = need("TENANT_ID");
  const purpose = need("PURPOSE");
  const apiKey = get("API_KEY");
  const bearerToken = get("BEARER");
  if (!apiKey && !bearerToken) problems.push(`${pre}API_KEY (or ${pre}BEARER) is not set`);

  const targets: Array<[string, string]> = [
    ["TARGET", target],
    ["AUTH_TARGET", authTarget],
    ["WEBRTC_TARGET", webrtcTarget],
  ];
  for (const [name, value] of targets) {
    if (value && !value.includes(":")) problems.push(`${pre}${name} must be host:port, got "${value}"`);
  }

  const credentials: UdbProjectConfig["credentials"] = {};
  if (apiKey) credentials.apiKey = apiKey;
  if (bearerToken) credentials.bearerToken = bearerToken;
  const config: UdbProjectConfig = { target, tenantId, purpose, credentials };
  if (authTarget) config.authTarget = authTarget;
  if (webrtcTarget) config.webrtcTarget = webrtcTarget;
  const projectId = get("PROJECT_ID") || get("PROJECT");
  if (projectId) config.projectId = projectId;
  const serviceIdentity = get("SERVICE_IDENTITY");
  if (serviceIdentity) config.serviceIdentity = serviceIdentity;
  const scopes = splitEnvScopes(get("SCOPES"));
  if (scopes) config.scopes = scopes;

  const rawDeadline = get("DEADLINE");
  if (rawDeadline) {
    const deadlineMs = parseDurationMs(rawDeadline);
    if (deadlineMs === undefined || deadlineMs <= 0) {
      problems.push(`${pre}DEADLINE must be a positive duration such as 10s, got "${rawDeadline}"`);
    } else {
      config.deadlineMs = deadlineMs;
    }
  }

  const tls = get("TLS");
  if (tls.toLowerCase() === "true" || tls === "1") {
    config.secure = true;
    const serverName = get("TLS_SERVER_NAME");
    if (serverName) {
      config.channelOptions = {
        "grpc.ssl_target_name_override": serverName,
        "grpc.default_authority": serverName,
      };
    }
  }
  if (problems.length) throw new Error(`udb: environment is incomplete: ${problems.join("; ")}`);
  return config;
}

/** What a service asserts about the principal it connected as. Every field is
 *  optional; set the ones the service relies on. */
export interface UdbExpect {
  tenantId?: string;
  projectId?: string;
  serviceIdentity?: string;
  /** Scopes that must all be granted, so a missing one fails at connect instead of
   *  on the first call that needs it. */
  requiredScopes?: string[];
}

/** Check a verified principal against `expect`, reporting every mismatch at once. */
export function verifyPrincipal(principal: any | null | undefined, expect: UdbExpect = {}): void {
  const required = expect.requiredScopes ?? [];
  if (!expect.tenantId && !expect.projectId && !expect.serviceIdentity && required.length === 0) return;
  if (!principal) {
    throw new Error(
      "udb: cannot verify the connection: it has no verified principal (connect with an API key, or use connectEnterprise)",
    );
  }
  const problems: string[] = [];
  const check = (label: string, actual: unknown, wanted?: string) => {
    if (wanted && (actual ?? "") !== wanted) problems.push(`${label} is "${actual ?? ""}", expected "${wanted}"`);
  };
  check("tenant", principal.tenant_id, expect.tenantId);
  check("project", principal.project_id, expect.projectId);
  check("service identity", principal.service_identity, expect.serviceIdentity);
  const granted = new Set<string>((principal.scopes ?? []).map((scope: string) => scope.toLowerCase()));
  const missing = required.filter((scope) => !granted.has(scope.toLowerCase()));
  if (missing.length) {
    problems.push(`the grant lacks scopes ${missing.join(", ")} (add them to the service account's grant)`);
  }
  if (problems.length) throw new Error(`udb: connected principal does not match: ${problems.join("; ")}`);
}

// grpc.health.v1 is not part of the SDK's proto tree. Its two messages are small
// enough to code here: an empty request asks about the whole server, and the reply
// holds one enum in field 1.
const HEALTH_CHECK_PATH = "/grpc.health.v1.Health/Check";
const HEALTH_STATUS_NAMES: Record<number, string> = {
  0: "UNKNOWN",
  1: "SERVING",
  2: "NOT_SERVING",
  3: "SERVICE_UNKNOWN",
};

function decodeHealthStatus(bytes: Buffer): number {
  let offset = 0;
  const varint = (): number => {
    let value = 0;
    let shift = 0;
    while (offset < bytes.length) {
      const byte = bytes[offset++];
      value += (byte & 0x7f) * 2 ** shift;
      if (!(byte & 0x80)) return value;
      shift += 7;
    }
    throw new Error("udb: truncated health check response");
  };
  let status = 0;
  while (offset < bytes.length) {
    const tag = varint();
    const wire = tag % 8;
    const value = wire === 0 ? varint() : undefined;
    if (wire === 2) offset += varint();
    else if (wire === 1) offset += 8;
    else if (wire === 5) offset += 4;
    else if (wire !== 0) throw new Error("udb: malformed health check response");
    if (tag === 8 && value !== undefined) status = value;
  }
  return status;
}

/** Run the standard gRPC health check for the whole server over `client`. Throws
 *  when the broker cannot answer or answers anything but SERVING. */
export async function checkBrokerHealth(
  client: grpc.Client,
  metadata: grpc.Metadata,
  options: { deadlineMs: number; signal?: AbortSignal },
): Promise<void> {
  const status = await new Promise<number>((resolve, reject) => {
    const call = client.makeUnaryRequest<Buffer, number>(
      HEALTH_CHECK_PATH,
      (request) => request,
      decodeHealthStatus,
      Buffer.alloc(0),
      metadata,
      { deadline: new Date(Date.now() + options.deadlineMs) },
      (err, value) => {
        if (err) reject(err);
        else resolve(value ?? 0);
      },
    );
    if (options.signal) {
      if (options.signal.aborted) call.cancel();
      else options.signal.addEventListener("abort", () => call.cancel(), { once: true });
    }
  }).catch((err: Error) => {
    throw new Error(`udb: broker health check failed: ${err.message}`, { cause: err });
  });
  if (status !== 1) throw new Error(`udb: broker is ${HEALTH_STATUS_NAMES[status] ?? status}`);
}
