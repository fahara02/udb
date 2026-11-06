// The browser module's storage, in Node's WebCrypto. The real module in a real browser is
// proven by scripts/mls_web_browser_smoke.mjs.
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import { forgetWebDevice, type MlsKeyValue, openWebDevice, saveWebDevice } from "./mlsWeb";

function memory(): MlsKeyValue & { values: Map<string, unknown> } {
  const values = new Map<string, unknown>();
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

function fakeWasm(): any {
  const device = (state: Uint8Array) => ({
    tenantId: "t",
    subjectId: "s",
    deviceId: "d",
    snapshot: () => state,
  });
  return {
    MlsDevice: {
      create: () => device(SECRET),
      restore: (snapshot: Uint8Array) => device(snapshot),
    },
  };
}

test("a device's state is sealed at rest and restores", async () => {
  const store = memory();
  const device = await openWebDevice(fakeWasm(), store, "t", "s", "d");
  await saveWebDevice(store, device);
  const saved = store.values.get("udb.mls.device:t/s/d") as { sealed: ArrayBuffer };
  assert.ok(saved, "the state is stored");
  assert.ok(!Buffer.from(saved.sealed).includes(Buffer.from(SECRET)), "the keys are not in clear");
  const key = store.values.get("udb.mls.seal-key") as CryptoKey;
  assert.equal(key.extractable, false, "script cannot read the sealing key out");
  const restored = await openWebDevice(fakeWasm(), store, "t", "s", "d");
  assert.deepEqual(Buffer.from(restored.snapshot()), Buffer.from(SECRET));
});

test("state that does not open throws rather than starting a new device", async () => {
  const store = memory();
  await openWebDevice(fakeWasm(), store, "t", "s", "d");
  store.values.delete("udb.mls.seal-key");
  await assert.rejects(openWebDevice(fakeWasm(), store, "t", "s", "d"));
});

test("forgetting a device removes its state", async () => {
  const store = memory();
  await openWebDevice(fakeWasm(), store, "t", "s", "d");
  await forgetWebDevice(store, "t", "s", "d");
  assert.equal(store.values.has("udb.mls.device:t/s/d"), false);
});

test("the browser module pulls in nothing from Node", () => {
  const compiled = fs.readFileSync(path.join(__dirname, "mlsWeb.js"), "utf8");
  assert.equal(/require\(/.test(compiled), false, "a browser bundle cannot satisfy require");
});
