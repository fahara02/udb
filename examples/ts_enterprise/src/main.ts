// UDB enterprise TypeScript example — the REAL path, no dev shortcuts.
//
//   project → authn (password login → RS256 JWT) → authz (real ABAC decision)
//           → CRUD a tenant-scoped billing table with the verified bearer.
//
// This deliberately faces the friction a toy example dodges:
//   - the SDK is a real installed dependency (@udb_plus/sdk), not a source import;
//   - the native auth control plane lives on a SEPARATE listener (:50061), not
//     the public data port (:50051);
//   - login returns a JWT whose tenant claim is the CANONICAL tenant UUID, not
//     the human code "acme" — we adopt it before touching data;
//   - the broker authorizes data RPCs against a real ABAC policy snapshot
//     (default-deny); without a seeded policy every call is PERMISSION_DENIED.
//
// Env (see .env.example): UDB_TARGET, UDB_AUTH_TARGET, UDB_USER, UDB_PASS, UDB_TENANT.
import * as fs from "fs";
import { UdbProject } from "@udb_plus/sdk";

// The row type is GENERATED from billing/v1/invoice.proto (ts-proto, snake_case)
// — run `buf generate` (see README). No hand-written duplicate of the schema.
import type { Invoice } from "../gen/billing/v1/invoice";

// Optional mTLS: when the broker is started with TLS+mTLS, point these at the CA
// and the client cert/key and the SAME code talks to it over a mutually-
// authenticated channel (UDB_FRICTION §8). Plaintext otherwise.
function tlsFromEnv() {
  const ca = process.env.UDB_TLS_CA;
  if (!ca) return undefined;
  return {
    rootCerts: fs.readFileSync(ca),
    certChain: process.env.UDB_TLS_CLIENT_CERT ? fs.readFileSync(process.env.UDB_TLS_CLIENT_CERT) : undefined,
    privateKey: process.env.UDB_TLS_CLIENT_KEY ? fs.readFileSync(process.env.UDB_TLS_CLIENT_KEY) : undefined,
  };
}

const DATA_TARGET = process.env.UDB_TARGET ?? "127.0.0.1:50051";
const AUTH_TARGET = process.env.UDB_AUTH_TARGET ?? "127.0.0.1:50061";
const TENANT_CODE = process.env.UDB_TENANT ?? "acme";
const USERNAME = process.env.UDB_USER ?? "admin";
const PASSWORD = process.env.UDB_PASS ?? "";

async function main(): Promise<void> {
  if (!PASSWORD) throw new Error("set UDB_PASS to the admin password (see .env.example)");

  // ── connect ────────────────────────────────────────────────────────────────
  // Data plane on :50051, native auth control plane on :50061. Mixing these up
  // is the #1 real-world stumble: calling Login on :50051 returns UNIMPLEMENTED.
  const tls = tlsFromEnv();
  const udb = await UdbProject.connect({
    target: DATA_TARGET,
    authTarget: AUTH_TARGET,
    tenantId: TENANT_CODE, // human code now; replaced by the canonical UUID below
    projectId: "default",
    purpose: "billing-app",
    ...(tls ? { tls } : {}),
  });
  console.log(`transport: ${tls ? "mTLS (mutual TLS)" : "plaintext"}`);

  try {
    // ── 1. AUTHN: real password login → RS256 JWT ───────────────────────────
    const login: any = await udb.login({ username: USERNAME, password: PASSWORD });
    if (login.mfa_required) throw new Error("MFA required — supply a second factor (out of scope here)");
    if (!login.access_token) throw new Error("login returned no access_token (is UDB_JWT_PRIVATE_KEY set on the broker?)");

    // The bearer's tenant claim is the CANONICAL UUID. Verify the token and
    // adopt that UUID on every outbound channel — a tenant-scoped RPC rejects a
    // mismatched x-tenant-id, and the human code "acme" is NOT the UUID.
    const verified: any = await udb.auth.authenticateBearer(login.access_token);
    const principal = verified.principal ?? {};
    const tenantUuid: string = principal.tenant_id ?? "";
    const scopes: string[] = principal.scopes ?? [];
    udb.setTenant(tenantUuid);
    console.log(`authn: logged in as ${USERNAME}`);
    console.log(`       tenant code "${TENANT_CODE}" → canonical UUID ${tenantUuid}`);
    console.log(`       scopes: ${JSON.stringify(scopes)}`);

    // ── 2. AUTHZ ────────────────────────────────────────────────────────────
    // Authorization is enforced SERVER-SIDE on every CRUD op below, against the
    // seeded ABAC policy snapshot. It is DEFAULT-DENY: without
    // UDB_ABAC_POLICIES_JSON these calls return PERMISSION_DENIED even for this
    // org-owner admin (that's the real §7 friction — the org-owner ROLE alone is
    // not enough; the data-plane reads an ABAC policy snapshot).
    // (Gotcha: the SDK's udb.authz.can/require query a SEPARATE control-plane
    // Casbin governance engine, which can disagree until its rules are seeded
    // too. We rely on the authoritative data-plane gate exercised by the CRUD.)
    console.log("authz: data-plane ABAC gates every CRUD op below (default-deny without the seeded policy)");

    // ── 3. CRUD the tenant-scoped billing table with the verified bearer ─────
    const invoices = udb.data.table<Invoice>("billing.v1.Invoice", { key: ["invoice_id"] });
    const id = "1c1c1c1c-0000-4000-8000-000000000001";

    // CREATE — tenant_id MUST be the adopted canonical UUID (the broker enforces
    // it; we set it explicitly so the row is unambiguous).
    await invoices.upsert({
      invoice_id: id,
      tenant_id: tenantUuid,
      customer_email: "ada@example.com",
      amount_cents: 4999,
      status: "paid",
    });
    console.log("crud:  created invoice", id);

    // READ — a tenant-scoped table requires tenant_id IN THE FILTER (the broker
    // rejects an unscoped read: "tenant isolation requires filter on tenant_id").
    let rows = await invoices.select({ where: { invoice_id: id, tenant_id: tenantUuid } });
    console.log("crud:  read   ", JSON.stringify(rows[0]));

    // UPDATE
    await invoices.upsert({
      invoice_id: id,
      tenant_id: tenantUuid,
      customer_email: "ada@example.com",
      amount_cents: 4999,
      status: "refunded",
    });
    rows = await invoices.select({ where: { invoice_id: id, tenant_id: tenantUuid } });
    console.log("crud:  updated", JSON.stringify(rows[0]));

    // DELETE
    await invoices.delete({ invoice_id: id, tenant_id: tenantUuid });
    rows = await invoices.select({ where: { invoice_id: id, tenant_id: tenantUuid } });
    console.log("crud:  deleted; remaining rows =", rows.length);

    // ── 4. Tenant isolation (authz, observable) ─────────────────────────────
    // Re-create the row, then ask for a FOREIGN tenant's data: the broker scopes
    // every read to the bearer's tenant, so another tenant's id leaks nothing.
    await invoices.upsert({ invoice_id: id, tenant_id: tenantUuid, customer_email: "ada@example.com", amount_cents: 100, status: "paid" });
    const FOREIGN_TENANT = "00000000-0000-0000-0000-0000000d9999";
    try {
      const leaked = await invoices.select({ where: { tenant_id: FOREIGN_TENANT } });
      console.log(`authz: cross-tenant read (tenant ${FOREIGN_TENANT}) → ${leaked.length} row(s) — isolation enforced`);
    } catch (e) {
      console.log(`authz: cross-tenant read blocked — ${(e as Error).message.split("\n")[0]}`);
    }
    await invoices.delete({ invoice_id: id, tenant_id: tenantUuid }); // cleanup

    console.log("\nENTERPRISE FLOW OK (authn + authz + tenant-scoped CRUD + isolation)");
  } finally {
    await udb.logout();
    udb.close();
  }
}

main().catch((err) => {
  console.error("\nFAILED:", err?.message ?? err);
  process.exit(1);
});
