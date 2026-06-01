// Admin-flow variant — the full native-services provisioning sequence from
// TypeScript, symmetric with the Go example: register a user, define RBAC
// (role → assignment → policy), verify the access check, and mint an API key the
// consumer example (main.ts) can authenticate.
//
// The `UdbAuthClient` wrapper covers the consumer surface; provisioning drives
// the raw Authn/Authz/ApiKey RPCs, loaded from the repo `proto/` dir via
// @grpc/proto-loader (the SDK loads stubs dynamically, so no codegen is needed).
//
// Run (see README): npm install && UDB_TARGET=127.0.0.1:50051 npx tsx admin.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { metadata, type UdbMetadata } from "../../../sdk/typescript/client";

const PROTO_ROOT = path.resolve(__dirname, "../../../proto");
const TARGET = process.env.UDB_TARGET ?? "127.0.0.1:50051";

function loadService(file: string, servicePath: string): any {
  const includeDirs = [PROTO_ROOT, path.resolve(PROTO_ROOT, "../third_party/googleapis")];
  const def = protoLoader.loadSync(file, {
    keepCase: true, longs: String, enums: String, defaults: true, oneofs: true, includeDirs,
  });
  const pkg = grpc.loadPackageDefinition(def) as any;
  return servicePath.split(".").reduce((o, k) => o[k], pkg);
}

function unary<T>(client: any, method: string, req: any, md: grpc.Metadata): Promise<T> {
  return new Promise((resolve, reject) =>
    client[method](req, md, (err: grpc.ServiceError | null, resp: T) => (err ? reject(err) : resolve(resp))),
  );
}

async function main(): Promise<void> {
  const meta: UdbMetadata = {
    tenantId: "acme", projectId: "billing", purpose: "control-plane",
    correlationId: "native-ts-admin", scopes: ["udb:*"],
    serviceIdentity: "examples.native-ts-admin", userId: "", clientCatalogVersion: "1.0.0",
  };
  const md = metadata(meta);
  const creds = grpc.credentials.createInsecure();
  const Authn = loadService("udb/core/authn/services/v1/authn_service.proto", "udb.core.authn.services.v1.AuthnService");
  const Authz = loadService("udb/core/authz/services/v1/authz_service.proto", "udb.core.authz.services.v1.AuthzService");
  const ApiKey = loadService("udb/core/apikey/services/v1/apikey_service.proto", "udb.core.apikey.services.v1.ApiKeyService");
  const authn = new Authn(TARGET, creds);
  const authz = new Authz(TARGET, creds);
  const apikey = new ApiKey(TARGET, creds);
  const suffix = String(Date.now());

  // ── Step 1: register a user ────────────────────────────────────────────────
  const created: any = await unary(authn, "CreateUser", {
    username: `alice_${suffix}`, email: `alice_${suffix}@example.com`, password: "CorrectHorse1!",
    tenant_id: "acme", full_name: "Alice Example", project_id: "billing",
  }, md);
  const userId = created.user.user_id;
  console.log(`1) registered user ${userId}`);

  // ── Step 2: RBAC — role → assignment → allow policy ────────────────────────
  const cr: any = await unary(authz, "CreateRole", {
    name: `Reader ${suffix}`, role_code: `reader_${suffix}`, created_by: userId,
    domain: "acme", tenant_id: "acme", project_id: "billing",
  }, md);
  const role = cr.role;
  await unary(authz, "AssignRole", {
    user_id: userId, role_id: role.role_id, domain: "acme", assigned_by: userId,
    tenant_id: "acme", project_id: "billing",
  }, md);
  await unary(authz, "PutAuthzPolicy", {
    policy: { id: `policy-${role.role_code}`, enabled: true, effect: "allow", tenant: "acme",
      project: "billing", role: role.role_code, action: "data.select", resource: "invoice" },
  }, md);
  console.log(`2) role ${role.role_code} assigned to user; allow policy on invoice/data.select added`);

  // ── Step 3: verify the access check ────────────────────────────────────────
  const ca: any = await unary(authz, "CheckAccess", {
    user_id: userId, domain: "acme", tenant_id: "acme", project_id: "billing",
    object: "invoice", action: "data.select",
  }, md);
  console.log(`3) check data.select on invoice → ${ca.allowed}`);

  // ── Step 4: mint an API key for the consumer example ───────────────────────
  const ck: any = await unary(apikey, "CreateApiKey", {
    name: "native-ts-admin-key", owner_id: userId, scopes: ["data:read"],
  }, md);
  console.log(`4) minted dev API key → export UDB_API_KEY=${ck.plain_key}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
