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
const node_path_1 = __importDefault(require("node:path"));
const stores_1 = require("../stores");
const CASES_PATH = node_path_1.default.join(__dirname, "..", "..", "..", "conformance", "table-cases.json");
const contract = JSON.parse(node_fs_1.default.readFileSync(CASES_PATH, "utf8"));
function substitute(values, id) {
    return Object.fromEntries(Object.entries(values ?? {}).map(([key, value]) => [key, typeof value === "string" ? value.replaceAll("$id", id) : value]));
}
async function runStoreCases(t, connect) {
    for (const tc of contract.cases) {
        await t.test(tc.name, async () => {
            const table = new stores_1.Table(connect(), contract.message_type, { key: contract.key });
            const id = (0, node_crypto_1.randomUUID)();
            for (const [index, step] of tc.steps.entries()) {
                let got;
                let error = "";
                try {
                    const key = substitute(step.key, id);
                    switch (step.op) {
                        case "upsert":
                            await table.upsert(substitute(step.row, id));
                            break;
                        case "get":
                            got = await table.get(key);
                            break;
                        case "patch":
                            await table.patch(key, substitute(step.changes, id));
                            break;
                        case "update_if":
                            await table.updateIf(key, substitute(step.expected, id), substitute(step.changes, id));
                            break;
                        case "delete":
                            await table.delete(key);
                            break;
                        default: strict_1.default.fail(`unknown op ${step.op}`);
                    }
                }
                catch (err) {
                    error = (0, stores_1.codeOf)(err);
                }
                strict_1.default.equal(error, step.expect?.error ?? "", `step ${index} (${step.op})`);
                if (step.expect?.row) {
                    strict_1.default.ok(got, `step ${index} returned a row`);
                    for (const [field, want] of Object.entries(substitute(step.expect.row, id))) {
                        strict_1.default.equal(got[field], want, `step ${index}: ${field}`);
                    }
                }
            }
        });
    }
}
exports.storeCaseKeys = { [contract.message_type]: contract.key };
