"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.storeCaseKeys = void 0;
exports.runStoreCases = runStoreCases;
const strict_1 = __importDefault(require("node:assert/strict"));
const node_crypto_1 = require("node:crypto");
const node_fs_1 = __importDefault(require("node:fs"));
const stores_1 = require("../stores");
const conformance_1 = require("../udbtest/conformance");
const contract = JSON.parse(node_fs_1.default.readFileSync((0, conformance_1.tableCasesPath)(), "utf8"));
/** Replaces "$id<digits>" ($id2, $id3, ...) with a fresh UUID of its own per case,
 *  before the bare "$id", as the udbtest runner does. */
function substituter(id) {
    const numbered = new Map();
    const text = (value) => value
        .replace(/\$id(\d+)/g, (_, n) => {
        if (!numbered.has(n))
            numbered.set(n, (0, node_crypto_1.randomUUID)());
        return numbered.get(n);
    })
        .replaceAll("$id", id);
    return (values) => Object.fromEntries(Object.entries(values ?? {}).map(([key, value]) => [key, typeof value === "string" ? text(value) : value]));
}
async function runStoreCases(t, connect) {
    for (const tc of contract.cases) {
        await t.test(tc.name, async () => {
            const table = new stores_1.Table(connect(), contract.message_type, { key: contract.key });
            const substitute = substituter((0, node_crypto_1.randomUUID)());
            for (const [index, step] of tc.steps.entries()) {
                let got;
                let affected;
                let error = "";
                try {
                    const key = substitute(step.key);
                    switch (step.op) {
                        case "upsert":
                            await table.upsert(substitute(step.row));
                            break;
                        case "get":
                            got = await table.get(key);
                            break;
                        case "patch":
                            await table.patch(key, substitute(step.changes));
                            break;
                        case "update_if":
                            await table.updateIf(key, substitute(step.expected), substitute(step.changes));
                            break;
                        case "delete":
                            await table.delete(key);
                            break;
                        case "bulk_upsert":
                            affected = (await table.bulkUpsert((step.rows ?? []).map((row) => substitute(row)))).affected_rows;
                            break;
                        default: strict_1.default.fail(`unknown op ${step.op}`);
                    }
                }
                catch (err) {
                    error = (0, stores_1.codeOf)(err);
                }
                strict_1.default.equal(error, step.expect?.error ?? "", `step ${index} (${step.op})`);
                if (step.expect?.affected_rows !== undefined) {
                    strict_1.default.equal(affected, step.expect.affected_rows, `step ${index}: affected_rows`);
                }
                if (step.expect?.row) {
                    strict_1.default.ok(got, `step ${index} returned a row`);
                    for (const [field, want] of Object.entries(substitute(step.expect.row))) {
                        strict_1.default.equal(got[field], want, `step ${index}: ${field}`);
                    }
                }
            }
        });
    }
}
exports.storeCaseKeys = { [contract.message_type]: contract.key };
