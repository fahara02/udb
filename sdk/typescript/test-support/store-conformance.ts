import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { TestContext } from "node:test";
import { Table, codeOf, type StorePlane } from "../stores";

interface CaseStep {
  op: string;
  key?: Record<string, unknown>;
  row?: Record<string, unknown>;
  changes?: Record<string, unknown>;
  expected?: Record<string, unknown>;
  expect?: { error?: string; row?: Record<string, unknown> };
}

const CASES_PATH = path.join(__dirname, "..", "..", "..", "conformance", "table-cases.json");
const contract = JSON.parse(fs.readFileSync(CASES_PATH, "utf8"));

function substitute(values: Record<string, unknown> | undefined, id: string): Record<string, unknown> {
  return Object.fromEntries(Object.entries(values ?? {}).map(([key, value]) =>
    [key, typeof value === "string" ? value.replaceAll("$id", id) : value]));
}

export async function runStoreCases(t: TestContext, connect: () => StorePlane): Promise<void> {
  for (const tc of contract.cases as { name: string; steps: CaseStep[] }[]) {
    await t.test(tc.name, async () => {
      const table = new Table<Record<string, unknown>>(connect(), contract.message_type, { key: contract.key });
      const id = randomUUID();
      for (const [index, step] of tc.steps.entries()) {
        let got: Record<string, unknown> | undefined;
        let error = "";
        try {
          const key = substitute(step.key, id);
          switch (step.op) {
            case "upsert": await table.upsert(substitute(step.row, id)); break;
            case "get": got = await table.get(key); break;
            case "patch": await table.patch(key, substitute(step.changes, id)); break;
            case "update_if": await table.updateIf(key, substitute(step.expected, id), substitute(step.changes, id)); break;
            case "delete": await table.delete(key); break;
            default: assert.fail(`unknown op ${step.op}`);
          }
        } catch (err) { error = codeOf(err); }
        assert.equal(error, step.expect?.error ?? "", `step ${index} (${step.op})`);
        if (step.expect?.row) {
          assert.ok(got, `step ${index} returned a row`);
          for (const [field, want] of Object.entries(substitute(step.expect.row, id))) {
            assert.equal(got[field], want, `step ${index}: ${field}`);
          }
        }
      }
    });
  }
}

export const storeCaseKeys: Record<string, string[]> = { [contract.message_type]: contract.key };
