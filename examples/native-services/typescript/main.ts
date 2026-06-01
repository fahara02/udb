// Progressive, simplest→advanced tour of UDB's native auth services from
// TypeScript, over the `UdbAuthClient` SDK wrapper (the consumer side):
//
//   1. Authenticate an API key → resolved principal (simplest).
//   2. Authorize a resource/action — `can` (allowed vs. denied).
//   3. Native DB fast-path grant — `nativeAccess` (advanced, Stage 2).
//
// Provisioning users/roles/keys is an admin concern — do it once with the Go
// example (examples/native-services/go) and export the key as UDB_API_KEY.
//
// Run (see README): npm install && UDB_API_KEY=udbk_... npx tsx main.ts
import { UdbAuthClient } from "../../../sdk/typescript/auth";
import type { UdbMetadata } from "../../../sdk/typescript/client";

const TARGET = process.env.UDB_TARGET ?? "127.0.0.1:50051";
const API_KEY = process.env.UDB_API_KEY ?? "";

async function main(): Promise<void> {
  const meta: UdbMetadata = {
    tenantId: "acme",
    projectId: "billing",
    purpose: "control-plane",
    correlationId: "native-ts-example",
    scopes: ["udb:*"],
    serviceIdentity: "examples.native-ts",
    userId: "",
    clientCatalogVersion: "1.0.0",
  };
  // protoRoot defaults to the repo `proto/` dir (resolved relative to the SDK).
  const auth = new UdbAuthClient(TARGET, meta);

  // ── Step 1 (simplest): authenticate an API key → principal ─────────────────
  if (API_KEY) {
    const resp: any = await auth.authenticateApiKey(API_KEY);
    console.log(`1) api key authenticated → user_id=${resp.principal?.user_id} scopes=${JSON.stringify(resp.principal?.scopes ?? [])}`);
  } else {
    console.log("1) set UDB_API_KEY (mint one with the Go example) to see authentication");
  }

  // ── Step 2: authorize a resource/action ────────────────────────────────────
  const invoice = { resource_name: "invoice", message_type: "invoice" };
  const [allowed, decision] = await auth.can(invoice, "data.select");
  console.log(`2) can data.select on invoice → ${allowed} (decision_id=${decision?.decision_id})`);
  const [denied] = await auth.can(invoice, "data.delete");
  console.log(`   can data.delete on invoice → ${denied}`);

  // ── Step 3 (advanced): native DB fast-path grant ───────────────────────────
  try {
    const grant: any = await auth.nativeAccess(invoice, "data.select");
    if (!grant) {
      console.log("3) access allowed; no native grant minted (server native-access not configured)");
    } else {
      console.log(`3) native grant: role=${grant.role} session_vars=${Object.keys(grant.session_variables ?? {}).length}`);
    }
  } catch (e) {
    console.log(`3) native access denied/unavailable: ${(e as Error).message}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
