#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

const repo = process.cwd();
const outDir = path.join(repo, "docs", "generated");
const nativeContractPath = path.join(outDir, "udb-native-contract.json");
const serviceRoots = [
  "proto/udb/core/authn/services/v1",
  "proto/udb/core/idp/services/v1",
  "proto/udb/core/authz/services/v1",
  "proto/udb/core/apikey/services/v1",
  "proto/udb/core/tenant/services/v1",
  "proto/udb/core/notification/services/v1",
  "proto/udb/core/analytics/services/v1",
  "proto/udb/core/storage/services/v1",
  "proto/udb/core/asset/services/v1",
  "proto/udb/core/webrtc/services/v1",
];
const sensitiveRoots = [
  "proto/udb/core/authn",
  "proto/udb/core/idp",
  "proto/udb/core/authz",
  "proto/udb/core/apikey",
];

function read(rel) {
  return fs.readFileSync(path.join(repo, rel), "utf8");
}

function loadNativeContract() {
  const parse = (text, source) => {
    try {
      const doc = JSON.parse(text);
      if (Array.isArray(doc.services) && doc.services.length > 0) {
        return doc;
      }
    } catch {
      // Fall through to regeneration.
    }
    if (source) {
      console.warn(`ignoring invalid native contract JSON at ${source}`);
    }
    return null;
  };

  if (fs.existsSync(nativeContractPath) && fs.statSync(nativeContractPath).size > 0) {
    const fromDisk = parse(fs.readFileSync(nativeContractPath, "utf8"), nativeContractPath);
    if (fromDisk) return fromDisk;
  }

  const output = execFileSync("cargo", ["run", "--quiet", "--", "native", "manifest"], {
    cwd: repo,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    stdio: ["ignore", "pipe", "inherit"],
  });
  const generated = parse(output, "udb native manifest");
  if (!generated) {
    throw new Error("udb native manifest produced no descriptor contract services");
  }
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(nativeContractPath, `${JSON.stringify(generated, null, 2)}\n`);
  return generated;
}

const nativeContract = loadNativeContract();

function contractMessagesByName(contract) {
  const messages = new Map();
  for (const message of contract.messages ?? []) {
    messages.set(message.message, message);
    messages.set(`.${message.message}`, message);
  }
  return messages;
}

const contractMessages = contractMessagesByName(nativeContract);

function listProtoFiles(rel) {
  const root = path.join(repo, rel);
  if (!fs.existsSync(root)) return [];
  const out = [];
  const walk = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile() && entry.name.endsWith(".proto")) {
        out.push(path.relative(repo, full).replaceAll("\\", "/"));
      }
    }
  };
  walk(root);
  return out.sort();
}

function lineNumber(text, index) {
  return text.slice(0, index).split(/\n/).length;
}

function braceEnd(text, openBraceIndex) {
  let depth = 0;
  for (let i = openBraceIndex; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === "{") depth += 1;
    else if (ch === "}") {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
}

function parsePackage(text) {
  return text.match(/^\s*package\s+([A-Za-z0-9_.]+)\s*;/m)?.[1] ?? "";
}

function parseMessages(files) {
  const messages = new Map();
  for (const rel of files) {
    const text = read(rel);
    const pkg = parsePackage(text);
    const re = /\bmessage\s+([A-Za-z0-9_]+)\s*\{/g;
    let match;
    while ((match = re.exec(text))) {
      const end = braceEnd(text, text.indexOf("{", match.index));
      if (end < 0) continue;
      const body = text.slice(text.indexOf("{", match.index) + 1, end);
      const fields = [];
      for (const line of body.split(/\r?\n/)) {
        const trimmed = line.trim();
        const field = trimmed.match(/^(?:optional\s+|repeated\s+|map<[^>]+>\s+)?[A-Za-z0-9_.]+\s+([A-Za-z0-9_]+)\s*=\s*\d+/);
        if (field) fields.push(field[1]);
      }
      messages.set(`${pkg}.${match[1]}`, { name: match[1], file: rel, fields });
      re.lastIndex = end + 1;
    }
  }
  return messages;
}

function parseEndpointSecurity(block) {
  const security = block.match(/option\s+\(udb\.core\.common\.v1\.endpoint_security\)\s*=\s*\{([\s\S]*?)\n\s*\};/);
  if (!security) return null;
  const body = security[1];
  return {
    mode: body.match(/mode:\s*([A-Z0-9_]+)/)?.[1] ?? "",
    scopes: [...body.matchAll(/scopes:\s*"([^"]+)"/g)].map((m) => m[1]),
    roles: [...body.matchAll(/roles:\s*"([^"]+)"/g)].map((m) => m[1]),
    policyRef: body.match(/policy_ref:\s*"([^"]+)"/)?.[1] ?? "",
    tenantRequired: /\btenant_required:\s*true\b/.test(body),
    csrfRequired: /\bcsrf_required:\s*true\b/.test(body),
    internalGrpcOnly: /\binternal_grpc_only:\s*true\b/.test(body),
    rateLimit: body.match(/rate_limit_policy_ref:\s*"([^"]+)"/)?.[1] ?? "",
    abusePolicy: body.match(/abuse_policy_ref:\s*"([^"]+)"/)?.[1] ?? "",
    decisionResource: body.match(/decision_resource:\s*"([^"]+)"/)?.[1] ?? "",
    tenantField: body.match(/tenant_field:\s*"([^"]+)"/)?.[1] ?? "",
  };
}

function handlerFor(serviceName) {
  if (serviceName === "AuthnService") return "src/runtime/service/auth_service/authn/";
  if (serviceName === "AuthzService") return "src/runtime/service/auth_service/authz/mod.rs";
  if (serviceName === "ApiKeyService") return "src/runtime/service/auth_service/apikey.rs";
  if (serviceName === "TenantService") return "src/runtime/service/tenant_service/mod.rs";
  if (serviceName === "NotificationService") return "src/runtime/service/notification_service/mod.rs";
  if (serviceName === "AnalyticsService") return "src/runtime/service/analytics_service/mod.rs";
  if (serviceName === "StorageService") return "src/runtime/service/storage_service/mod.rs";
  if (serviceName === "AssetService") return "src/runtime/service/asset_service/mod.rs";
  if (["RoomService", "PeerService", "TrackService", "TurnService", "SignalingService"].includes(serviceName)) {
    return "src/runtime/service/webrtc_service/";
  }
  return "";
}

function testRefsFor(serviceName) {
  if (serviceName === "AuthnService") return "src/runtime/service/auth_service/tests/authn_*";
  if (serviceName === "AuthzService") return "src/runtime/service/auth_service/tests/authz_*";
  if (serviceName === "ApiKeyService") return "src/runtime/service/auth_service/tests/apikey_live.rs";
  if (serviceName === "TenantService") return "src/runtime/service/auth_service/tests/tenant_live.rs";
  if (serviceName === "NotificationService") return "src/runtime/service/auth_service/tests/notification_*";
  if (serviceName === "AnalyticsService") return "src/runtime/service/auth_service/tests/analytics_live.rs";
  if (serviceName === "StorageService") return "src/runtime/service/auth_service/tests/storage_*";
  if (serviceName === "AssetService") return "src/runtime/service/auth_service/tests/asset_*";
  if (["RoomService", "PeerService", "TrackService", "TurnService", "SignalingService"].includes(serviceName)) {
    return "src/runtime/service/auth_service/tests/webrtc_live.rs";
  }
  return "";
}

function parseRpcs() {
  const rows = [];
  for (const service of nativeContract.services ?? []) {
    if (!service.service?.startsWith("udb.core.")) continue;
    const serviceName = service.service.split(".").pop() ?? service.service;
    for (const rpc of service.rpcs ?? []) {
      const requestType = String(rpc.input ?? "").replace(/^\./, "");
      const responseType = String(rpc.output ?? "").replace(/^\./, "");
      const requestFields = (contractMessages.get(requestType)?.fields ?? []).map((field) => field.name);
      rows.push({
        protoFile: service.file ?? "",
        line: "-",
        service: service.service,
        serviceName,
        method: rpc.method,
        requestType,
        responseType,
        tenantFields: requestFields.filter((field) => field === "tenant_id" || field.endsWith("_tenant_id")),
        security: endpointSecurityFromContract(rpc.endpoint_security),
        handler: handlerFor(serviceName),
        tests: testRefsFor(serviceName),
      });
    }
  }
  rows.sort((a, b) => `${a.service}/${a.method}`.localeCompare(`${b.service}/${b.method}`));
  return rows;
}

function endpointSecurityFromContract(security) {
  if (!security) return null;
  return {
    mode: security.auth_mode ?? "",
    scopes: security.scopes ?? [],
    roles: security.roles ?? [],
    policyRef: security.policy_ref ?? "",
    tenantRequired: Boolean(security.tenant_required),
    csrfRequired: Boolean(security.csrf_required),
    internalGrpcOnly: Boolean(security.internal_grpc_only),
    rateLimit: security.rate_limit_policy_ref ?? "",
    abusePolicy: security.abuse_policy_ref ?? "",
    decisionResource: security.decision_resource ?? "",
    tenantField: security.tenant_field ?? "",
  };
}

function parseSensitiveFields() {
  const rows = [];
  const sensitivePrefixes = sensitiveRoots.flatMap((root) => [
    `${root}/`,
    `${root.replace(/^proto\//, "")}/`,
  ]);
  for (const message of nativeContract.messages ?? []) {
    const rel = message.file ?? "";
    if (!sensitivePrefixes.some((prefix) => rel.startsWith(prefix))) continue;
    for (const field of message.fields ?? []) {
      const name = field.name ?? "";
      const scalar = field.scalar_security ?? {};
      const column = field.column_security ?? null;
      const lower = name.toLowerCase();
      const looksSensitive = /(password|secret|token|hash|credential|api_key|otp|csrf|email|phone|subject|address|jti|key)/.test(lower);
      const annotated = Boolean(
        column
        || scalar.sensitive
        || scalar.encrypted_security
        || scalar.log_redacted
        || scalar.log_masked
        || scalar.pii
        || scalar.requires_consent
        || scalar.data_purpose
        || scalar.security_classification
        || scalar.data_category
      );
      if (!looksSensitive && !annotated) continue;
      const explicitRedaction = Boolean(column || scalar.log_redacted || scalar.log_masked || scalar.pii);
      rows.push({
        protoFile: rel,
        line: "-",
        message: message.message,
        field: name,
        type: field.type ?? "",
        pii: Boolean(scalar.pii),
        sensitive: Boolean(scalar.sensitive),
        encrypted: Boolean(scalar.encrypted_security),
        logRedacted: Boolean(scalar.log_redacted),
        logMasked: Boolean(scalar.log_masked),
        structured: Boolean(column),
        storageOnly: Boolean(column?.output_view === 4 || column?.output_view === "OUTPUT_VIEW_STORAGE_ONLY"),
        explicitRedaction,
        redactionRule: redactionRuleFromContract(name, scalar, column, explicitRedaction),
        runtimeSurfaces: runtimeSurfaces(message.message, name),
      });
    }
  }
  rows.sort((a, b) => `${a.message}.${a.field}`.localeCompare(`${b.message}.${b.field}`));
  return rows;
}

function redactionRuleFromContract(name, scalar, column, explicitRedaction) {
  if (column?.output_view === 4 || column?.output_view === "OUTPUT_VIEW_STORAGE_ONLY") {
    return "storage-only, redacted";
  }
  if (scalar.log_redacted) return "redact in logs";
  if (scalar.log_masked) return "mask in logs";
  if (scalar.pii) return "PII masking required";
  if (column) return "structured column security; redact according to descriptor contract";
  return redactionRule(name, "", explicitRedaction);
}

function redactionRule(name, options, explicitRedaction) {
  if (/output_view:\s*OUTPUT_VIEW_STORAGE_ONLY/.test(options)) return "storage-only, redacted";
  if (/\(udb\.core\.common\.v1\.log_redacted\)\s*=\s*true/.test(options)) return "redact in logs";
  if (/\(udb\.core\.common\.v1\.log_masked\)\s*=\s*true/.test(options)) return "mask in logs";
  if (/\(udb\.core\.common\.v1\.pii\)\s*=\s*true/.test(options)) return "PII masking required";
  const suffix = explicitRedaction ? "" : "; proto redaction annotation pending";
  if (/(current_password|new_password|password)$/i.test(name) || /^password$/i.test(name)) {
    return `request credential; redact in logs and never persist plaintext${suffix}`;
  }
  if (/(plain_key|api_key|bearer_token|access_token|refresh_token|session_token|csrf_token|external_token|jwt|token)$/i.test(name)) {
    return `token material; redact in logs/CLI, one-time reveal only where explicit${suffix}`;
  }
  if (/(otp|totp|mfa|recovery_code|code|qr|challenge|secret)/i.test(name)) {
    return `authenticator secret or challenge; redact in logs/CLI and limit ceremony output${suffix}`;
  }
  if (/(hash|lookup|jti|credential_id|key_id|kid|key)/i.test(name)) {
    return `credential lookup/hash identifier; redact or minimize outside storage/admin audit${suffix}`;
  }
  if (/(email|phone|ip|address|subject|principal|identity|contact)/i.test(name)) {
    return `PII or subject identifier; mask in logs and tenant-scope API output${suffix}`;
  }
  return `classified for review; minimize logs and API output${suffix}`;
}

function runtimeSurfaces(message, field) {
  if (message.endsWith(".User")) return "Authn DTO mapping, auth events, SDK generated entity, logs";
  if (message.endsWith(".Session")) return "Session DTO mapping, token validation, SDK generated entity, logs";
  if (message.endsWith(".ApiKey")) return "API-key DTO mapping, CLI/API-key events, SDK generated entity, logs";
  if (message.endsWith(".Otp") || message.endsWith(".RecoveryCode") || message.endsWith(".WebAuthnCredential")) {
    return "Authn lifecycle handlers, events, SDK generated entity, logs";
  }
  if (message.includes(".authz.")) return "Authz admin APIs, access decision audit, policy bundle, SDK generated entity";
  return "Generated SDK entity, handler DTOs/events/logs";
}

function mdEscape(value) {
  return String(value).replaceAll("|", "\\|").replaceAll("\n", " ");
}

function writeRpcInventory(rows) {
  const annotated = rows.filter((r) => r.security).length;
  const publicCount = rows.filter((r) => authModeIsPublic(r.security?.mode)).length;
  const nonPublicWithoutAuthz = rows.filter((r) => r.security && !authModeIsPublic(r.security.mode) && r.security.scopes.length === 0 && r.security.roles.length === 0 && !r.security.policyRef).length;
  const signal = rows.find((r) => r.service.endsWith(".SignalingService") && r.method === "Signal");
  const lines = [];
  lines.push("# Authn/Authz RPC Inventory");
  lines.push("");
  lines.push("Generated by `node scripts/generate-authn-authz-inventory.mjs`. Do not hand-edit.");
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- Native RPCs inventoried: ${rows.length}`);
  lines.push(`- RPCs with endpoint_security: ${annotated}`);
  lines.push(`- RPCs without endpoint_security: ${rows.length - annotated}`);
  lines.push(`- Public bootstrap RPCs: ${publicCount}`);
  lines.push(`- Non-public RPCs missing scopes/roles/policy_ref: ${nonPublicWithoutAuthz}`);
  lines.push(`- WebRTC SignalingService.Signal endpoint security: ${signal?.security ? "present" : "missing"}`);
  lines.push("");
  lines.push("## RPCs");
  lines.push("");
  lines.push("| Service | Method | Proto | Endpoint security | Tenant fields | Request | Response | Handler | Tests |");
  lines.push("|---|---|---|---|---|---|---|---|---|");
  for (const r of rows) {
    const sec = r.security
      ? `${r.security.mode}; scopes=${r.security.scopes.join(",") || "-"}; roles=${r.security.roles.join(",") || "-"}; policy=${r.security.policyRef || "-"}; tenant_required=${r.security.tenantRequired}; rate=${r.security.rateLimit || "-"}; abuse=${r.security.abusePolicy || "-"}`
      : "MISSING";
    lines.push(`| ${mdEscape(r.service)} | ${mdEscape(r.method)} | ${r.protoFile}:${r.line} | ${mdEscape(sec)} | ${mdEscape(r.tenantFields.join(", ") || "-")} | ${mdEscape(r.requestType)} | ${mdEscape(r.responseType)} | ${mdEscape(r.handler)} | ${mdEscape(r.tests)} |`);
  }
  return `${lines.join("\n")}\n`;
}

function authModeIsPublic(mode) {
  return String(mode ?? "").toLowerCase() === "public" || mode === "AUTH_MODE_PUBLIC";
}

function writeSensitiveInventory(rows) {
  const missingExplicit = rows.filter((r) => !r.explicitRedaction).length;
  const structured = rows.filter((r) => r.structured).length;
  const lines = [];
  lines.push("# Authn/Authz Sensitive Field Inventory");
  lines.push("");
  lines.push("Generated by `node scripts/generate-authn-authz-inventory.mjs`. Do not hand-edit.");
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- Sensitive-looking or annotated fields inventoried: ${rows.length}`);
  lines.push(`- Fields with structured db_column_security: ${structured}`);
  lines.push(`- Fields missing explicit proto redaction annotation: ${missingExplicit}`);
  lines.push("");
  lines.push("## Fields");
  lines.push("");
  lines.push("| Message | Field | Proto | Type | Classification options | Redaction rule | Runtime/API/SDK surfaces |");
  lines.push("|---|---|---|---|---|---|---|");
  for (const r of rows) {
    const opts = [
      r.pii ? "pii" : "",
      r.sensitive ? "sensitive" : "",
      r.encrypted ? "encrypted_security" : "",
      r.logRedacted ? "log_redacted" : "",
      r.logMasked ? "log_masked" : "",
      r.structured ? "db_column_security" : "",
      r.storageOnly ? "storage_only" : "",
    ].filter(Boolean).join(", ") || "-";
    lines.push(`| ${mdEscape(r.message)} | ${mdEscape(r.field)} | ${r.protoFile}:${r.line} | ${mdEscape(r.type)} | ${mdEscape(opts)} | ${mdEscape(r.redactionRule)} | ${mdEscape(r.runtimeSurfaces)} |`);
  }
  lines.push("");
  lines.push("## Baseline Coverage");
  lines.push("");
  lines.push("- Live no-leak tests: `src/runtime/service/auth_service/tests/authn_noleak_live.rs` verifies stored user/session credential fields are blanked in DTO responses.");
  lines.push("- CLI redaction baseline: `src/cli/output.rs` redacts token, session, CSRF, TOTP, OTP, operation, and API-key secret output keys by default.");
  lines.push("- Remaining missing explicit annotations are baseline findings for later Phase F/G annotation and DTO hardening work.");
  return `${lines.join("\n")}\n`;
}

fs.mkdirSync(outDir, { recursive: true });
const rpcRows = parseRpcs();
const sensitiveRows = parseSensitiveFields();
fs.writeFileSync(path.join(outDir, "authn-authz-rpc-inventory.md"), writeRpcInventory(rpcRows));
fs.writeFileSync(path.join(outDir, "authn-authz-sensitive-fields.md"), writeSensitiveInventory(sensitiveRows));
console.log(`wrote ${rpcRows.length} RPC rows and ${sensitiveRows.length} sensitive field rows`);
