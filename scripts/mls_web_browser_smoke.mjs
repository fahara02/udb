// Run the web build of the MLS module in headless Chrome: two devices join a group and exchange
// a message, and a device's state survives a save to IndexedDB and a restore.
//
// Usage: node scripts/mls_web_browser_smoke.mjs <chrome binary>
// Needs: npm run build:mls-wasm and npm run build in sdk/typescript.
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sdk = path.join(repo, "sdk", "typescript");
const chrome = process.argv[2] || process.env.CHROME_BIN || "google-chrome";

const page = `<!doctype html><meta charset="utf-8">
<script type="module">
import init, * as bindings from "/mls-wasm-web/udb_mls_wasm.js";
const report = (text) => fetch("/result", { method: "POST", body: text });
try {
  const shim = { exports: {} };
  const source = await (await fetch("/dist/mlsWeb.js")).text();
  new Function("exports", "module", source)(shim.exports, shim);
  const { loadMlsWeb, indexedDbKeyValue, openWebDevice, saveWebDevice } = shim.exports;
  const wasm = await loadMlsWeb(init, bindings);
  const store = indexedDbKeyValue("udb-smoke-" + crypto.randomUUID());
  const tenant = "00000000-0000-0000-0000-00000000000a";
  const alice = await openWebDevice(wasm, store, tenant, "alice", "a-1");
  let bob = await openWebDevice(wasm, store, tenant, "bob", "b-1");
  alice.createGroup("room");
  const commit = alice.commitAdd("room", bob.keyPackages(1));
  alice.confirmCommit("room");
  bob.process(1, "WELCOME", "room", commit.epoch, commit.welcome, "a-1", "");
  await saveWebDevice(store, bob);
  // A reload: the device comes back from IndexedDB and still reads the group.
  bob = await openWebDevice(wasm, store, tenant, "bob", "b-1");
  const sealed = alice.encrypt("room", new TextEncoder().encode("hello from the browser"));
  const text = bob.process(2, "MESSAGE", "room", sealed.epoch, sealed.ciphertext, "a-1", "m-1");
  const plain = new TextDecoder().decode(text.plaintext);
  await report(plain === "hello from the browser" ? "OK" : "WRONG: " + plain);
} catch (err) {
  await report("FAILED: " + (err && err.stack || err));
}
</script>`;

let finish;
const finished = new Promise((resolve) => {
  finish = resolve;
});

const types = { ".js": "text/javascript", ".wasm": "application/wasm", ".html": "text/html" };
const server = http.createServer((request, response) => {
  const url = new URL(request.url, "http://localhost");
  if (url.pathname === "/result") {
    let body = "";
    request.on("data", (chunk) => {
      body += chunk;
    });
    request.on("end", () => {
      response.writeHead(204);
      response.end();
      finish(body);
    });
    return;
  }
  if (url.pathname === "/") {
    response.writeHead(200, { "content-type": types[".html"] });
    response.end(page);
    return;
  }
  const file = path.join(sdk, path.normalize(url.pathname));
  if (!file.startsWith(sdk) || !fs.existsSync(file)) {
    response.writeHead(404);
    response.end();
    return;
  }
  response.writeHead(200, { "content-type": types[path.extname(file)] ?? "application/octet-stream" });
  fs.createReadStream(file).pipe(response);
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const port = server.address().port;

const browser = spawn(
  chrome,
  ["--headless=new", "--no-sandbox", "--disable-gpu", "--remote-debugging-port=0", `http://127.0.0.1:${port}/`],
  { stdio: "ignore" },
);
browser.on("close", (code) => finish(`chrome exited ${code} before the page reported`));
const timer = setTimeout(() => finish("the page reported nothing in 120 seconds"), 120_000);
const outcome = await finished;
clearTimeout(timer);
browser.removeAllListeners("close");
browser.kill();
server.close();
server.closeAllConnections();
if (outcome !== "OK") {
  console.error(`mls web smoke: ${outcome}`);
  process.exit(1);
}
console.log("mls web smoke: OK");
process.exit(0);
