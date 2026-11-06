"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// The browser module's storage, in Node's WebCrypto. The real module in a real browser is
// proven by scripts/mls_web_browser_smoke.mjs.
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
const mlsWeb_1 = require("./mlsWeb");
function memory() {
    const values = new Map();
    return {
        values,
        get: async (key) => values.get(key),
        put: async (key, value) => {
            values.set(key, value);
        },
        delete: async (key) => {
            values.delete(key);
        },
    };
}
const SECRET = new TextEncoder().encode("private keys");
function fakeWasm() {
    const device = (state) => ({
        tenantId: "t",
        subjectId: "s",
        deviceId: "d",
        snapshot: () => state,
    });
    return {
        MlsDevice: {
            create: () => device(SECRET),
            restore: (snapshot) => device(snapshot),
        },
    };
}
(0, node_test_1.test)("a device's state is sealed at rest and restores", async () => {
    const store = memory();
    const device = await (0, mlsWeb_1.openWebDevice)(fakeWasm(), store, "t", "s", "d");
    await (0, mlsWeb_1.saveWebDevice)(store, device);
    const saved = store.values.get("udb.mls.device:t/s/d");
    strict_1.default.ok(saved, "the state is stored");
    strict_1.default.ok(!Buffer.from(saved.sealed).includes(Buffer.from(SECRET)), "the keys are not in clear");
    const key = store.values.get("udb.mls.seal-key");
    strict_1.default.equal(key.extractable, false, "script cannot read the sealing key out");
    const restored = await (0, mlsWeb_1.openWebDevice)(fakeWasm(), store, "t", "s", "d");
    strict_1.default.deepEqual(Buffer.from(restored.snapshot()), Buffer.from(SECRET));
});
(0, node_test_1.test)("state that does not open throws rather than starting a new device", async () => {
    const store = memory();
    await (0, mlsWeb_1.openWebDevice)(fakeWasm(), store, "t", "s", "d");
    store.values.delete("udb.mls.seal-key");
    await strict_1.default.rejects((0, mlsWeb_1.openWebDevice)(fakeWasm(), store, "t", "s", "d"));
});
(0, node_test_1.test)("forgetting a device removes its state", async () => {
    const store = memory();
    await (0, mlsWeb_1.openWebDevice)(fakeWasm(), store, "t", "s", "d");
    await (0, mlsWeb_1.forgetWebDevice)(store, "t", "s", "d");
    strict_1.default.equal(store.values.has("udb.mls.device:t/s/d"), false);
});
(0, node_test_1.test)("the browser module pulls in nothing from Node", () => {
    const compiled = node_fs_1.default.readFileSync(node_path_1.default.join(__dirname, "mlsWeb.js"), "utf8");
    strict_1.default.equal(/require\(/.test(compiled), false, "a browser bundle cannot satisfy require");
});
