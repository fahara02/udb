// Canonical native-contract metadata -> literal generated Go Client method docs.
// This helper changes comments only; Buf remains the RPC/signature producer.

function text(value, context) {
  if (typeof value !== "string" || /[\r\n\u0000-\u001f\u2028\u2029]/u.test(value)) {
    throw new Error(`invalid contract text: ${context}`);
  }
  return value;
}

export function credentialTypeNames(goSource) {
  const match = /CredentialType_name\s*=\s*map\[int32\]string\s*\{([\s\S]*?)\n\s*\}/u.exec(goSource);
  if (!match) throw new Error("generated CredentialType_name enum is missing");
  const names = new Map();
  for (const entry of match[1].matchAll(/(\d+):\s*"(CREDENTIAL_TYPE_[A-Z_]+)"/gu)) {
    names.set(Number(entry[1]), entry[2].replace(/^CREDENTIAL_TYPE_/u, ""));
  }
  if (!names.size) throw new Error("generated CredentialType_name enum is empty");
  return names;
}

function listeners(service, rpc) {
  const native = service.native_service;
  if (rpc.endpoint_security?.internal_grpc_only) return "internal loopback";
  if (!native && service.service === "udb.services.v1.DataBroker") return "data plane";
  const admitted = [];
  if (native?.public_listener_allowed) admitted.push("data plane");
  if (native?.control_plane_listener_allowed) admitted.push("control plane");
  if (native?.peer_listener_allowed) admitted.push("peer");
  if (!admitted.length) throw new Error(`no canonical listener for ${rpc.path}`);
  return admitted.join(", ");
}

function idempotency(rpc) {
  if (rpc.kind !== "unary") {
    return "streams are not automatically replayed; use the RPC resume contract.";
  }
  if (rpc.operation_kind === "read_only") {
    return "read-only unary calls may retry transient failures.";
  }
  const contract = rpc.idempotency_contract;
  if (contract?.replay_safe && contract.request_key_field) {
    return "automatic transient retry requires the declared request key; reuse it only for unchanged request semantics and the same tenant/project.";
  }
  return "no automatic mutation replay; a server-generated key alone does not permit retry.";
}

export function methodContractLines(service, rpc, names) {
  const scopes = [...new Set(rpc.scopes)].sort().map((scope) => text(scope, `${rpc.path} scope`));
  const allowed = rpc.endpoint_security?.allowed_credential_types ?? [];
  const credentials = [...new Set(allowed)].sort((a, b) => a - b).map((code) => {
    const name = names.get(code);
    if (!name) throw new Error(`unknown credential type ${code} for ${rpc.path}`);
    return `${name} (${code})`;
  });
  const isPublic = rpc.auth_mode === "public";
  const lines = [
    `UDB contract: ${rpc.path}`,
    `Listener: ${listeners(service, rpc)}.`,
    `Scopes: ${scopes.length ? scopes.join(", ") : isPublic ? "no endpoint scopes (PUBLIC); method-specific authorization still applies" : "listener and resource policy defaults (no explicit endpoint scopes)"}.`,
    `Credential types: ${credentials.length ? credentials.join(", ") : isPublic ? "PUBLIC endpoint; method-specific request credentials" : "listener credential policy defaults (no explicit allowlist)"}.`,
    `Operation kind: ${rpc.operation_kind}.`,
    `Idempotency: ${idempotency(rpc)}`,
  ];
  const contract = rpc.idempotency_contract;
  if (contract) {
    lines.push(`Idempotency fields: request key=${text(contract.request_key_field, `${rpc.path} request key`) || "none"}; server-generated key=${contract.server_generated_key}; duplicate response=${text(contract.duplicate_response_field, `${rpc.path} duplicate field`) || "none"}; replay-safe=${contract.replay_safe}.`);
  } else {
    lines.push("Idempotency fields: no declared method replay contract.");
  }
  if (rpc.endpoint_security?.idempotency_required) {
    lines.push("The endpoint requires an idempotency key.");
  }
  if (rpc.path === "/udb.services.v1.DataBroker/BeginTx") {
    lines.push("BeginTx validates transaction guards and per-mutation keys for upsert, update, delete and vector_upsert; unsupported keyed operations are refused.");
    lines.push("Relational mutation keys use durable replay receipts; changed replay inputs refuse the whole transaction.");
  }
  lines.push("End UDB contract.");
  return lines;
}

export class GoRPCContractDocs {
  constructor(manifest, names) {
    if (!Array.isArray(manifest.services) || manifest.service_count !== manifest.services.length) {
      throw new Error("canonical service_count does not match services");
    }
    this.methods = new Map();
    this.seen = new Set();
    this.names = names;
    for (const service of manifest.services) {
      if (!Array.isArray(service.rpcs) || service.rpc_count !== service.rpcs.length) {
        throw new Error(`canonical rpc_count does not match ${service.service}`);
      }
      for (const rpc of service.rpcs) {
        if (rpc.path !== `/${service.service}/${rpc.method}` || !/^\/udb\.[\w.]+\/[A-Za-z]\w*$/u.test(rpc.path)) {
          throw new Error(`invalid canonical method identity: ${rpc.path}`);
        }
        if (this.methods.has(rpc.path)) throw new Error(`duplicate canonical method: ${rpc.path}`);
        if (!Array.isArray(rpc.scopes) || !["unary", "bidi", "server_streaming", "client_streaming"].includes(rpc.kind)) {
          throw new Error(`incomplete canonical method: ${rpc.path}`);
        }
        if (!["read_only", "mutation", "destructive", "unspecified"].includes(rpc.operation_kind)) {
          throw new Error(`invalid operation kind: ${rpc.path}`);
        }
        this.methods.set(rpc.path, { service, rpc });
        // Validate all comment inputs before writing any generated file.
        methodContractLines(service, rpc, names);
      }
    }
  }

  annotate(source, filename) {
    const clients = new Map();
    for (const match of source.matchAll(/^\s*(\w+)_FullMethodName\s*=\s*"(\/udb\.[^"]+)"/gmu)) {
      const wirePath = match[2];
      const slash = wirePath.lastIndexOf("/");
      const wireMethod = wirePath.slice(slash + 1);
      const suffix = `_${wireMethod}`;
      if (!match[1].endsWith(suffix)) throw new Error(`ambiguous full-method constant in ${filename}`);
      const client = `${match[1].slice(0, -suffix.length)}Client`;
      const service = wirePath.slice(1, slash);
      if (clients.has(client) && clients.get(client) !== service) throw new Error(`ambiguous Client service in ${filename}`);
      clients.set(client, service);
    }
    // Replace only our bounded blocks, preserving every proto/deprecation comment.
    const clean = source.replace(/^\t\/\/ UDB contract: [^\n]+\n(?:\t\/\/[^\n]*\n)*?\t\/\/ End UDB contract\.\n/gmu, "");
    return clean.replace(/^type (\w+Client) interface \{\n([\s\S]*?)^\}/gmu, (whole, client, body) => {
      const service = clients.get(client);
      if (!service) throw new Error(`missing full-method identity for ${client} in ${filename}`);
      const documented = body.replace(/^(\t)([A-Z]\w*)\([^\n]+$/gmu, (signature, indent, method) => {
        const wirePath = `/${service}/${method}`;
        const contract = this.methods.get(wirePath);
        if (!contract) throw new Error(`generated method has no canonical contract: ${wirePath}`);
        if (this.seen.has(wirePath)) throw new Error(`duplicate generated method: ${wirePath}`);
        this.seen.add(wirePath);
        const comments = methodContractLines(contract.service, contract.rpc, this.names)
          .map((line) => `${indent}// ${line}\n`).join("");
        return comments + signature;
      });
      return `type ${client} interface {\n${documented}}`;
    });
  }

  finish() {
    const missing = [...this.methods.keys()].filter((method) => !this.seen.has(method));
    if (missing.length) throw new Error(`canonical methods missing generated Client docs: ${missing.join(", ")}`);
    return this.seen.size;
  }
}
