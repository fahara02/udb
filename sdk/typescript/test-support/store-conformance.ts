import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import type { TestContext } from "node:test";
import { Table, codeOf, type StorePlane } from "../stores";
import { tableCasesPath } from "../udbtest/conformance";

interface CaseStep {
  op: string;
  key?: Record<string, unknown>;
  row?: Record<string, unknown>;
  changes?: Record<string, unknown>;
  expected?: Record<string, unknown>;
  rows?: Record<string, unknown>[];
  expect?: { error?: string; row?: Record<string, unknown>; affected_rows?: number };
}

const contract = JSON.parse(fs.readFileSync(tableCasesPath(), "utf8"));

/** Replaces "$id<digits>" ($id2, $id3, ...) with a fresh UUID of its own per case,
 *  before the bare "$id", as the udbtest runner does. */
function substituter(id: string): (values: Record<string, unknown> | undefined) => Record<string, unknown> {
  const numbered = new Map<string, string>();
  const text = (value: string): string =>
    value
      .replace(/\$id(\d+)/g, (_, n: string) => {
        if (!numbered.has(n)) numbered.set(n, randomUUID());
        return numbered.get(n)!;
      })
      .replaceAll("$id", id);
  return (values) =>
    Object.fromEntries(Object.entries(values ?? {}).map(([key, value]) => [key, typeof value === "string" ? text(value) : value]));
}

export async function runStoreCases(t: TestContext, connect: () => StorePlane): Promise<void> {
  for (const tc of contract.cases as { name: string; steps: CaseStep[] }[]) {
    await t.test(tc.name, async () => {
      const table = new Table<Record<string, unknown>>(connect(), contract.message_type, { key: contract.key });
      const substitute = substituter(randomUUID());
      for (const [index, step] of tc.steps.entries()) {
        let got: Record<string, unknown> | undefined;
        let affected: number | undefined;
        let error = "";
        try {
          const key = substitute(step.key);
          switch (step.op) {
            case "upsert": await table.upsert(substitute(step.row)); break;
            case "get": got = await table.get(key); break;
            case "patch": await table.patch(key, substitute(step.changes)); break;
            case "update_if": await table.updateIf(key, substitute(step.expected), substitute(step.changes)); break;
            case "delete": await table.delete(key); break;
            case "bulk_upsert":
              affected = (await table.bulkUpsert((step.rows ?? []).map((row) => substitute(row)))).affected_rows;
              break;
            default: assert.fail(`unknown op ${step.op}`);
          }
        } catch (err) { error = codeOf(err); }
        assert.equal(error, step.expect?.error ?? "", `step ${index} (${step.op})`);
        if (step.expect?.affected_rows !== undefined) {
          assert.equal(affected, step.expect.affected_rows, `step ${index}: affected_rows`);
        }
        if (step.expect?.row) {
          assert.ok(got, `step ${index} returned a row`);
          for (const [field, want] of Object.entries(substitute(step.expect.row))) {
            assert.equal(got[field], want, `step ${index}: ${field}`);
          }
        }
      }
    });
  }
}

export const storeCaseKeys: Record<string, string[]> = { [contract.message_type]: contract.key };
