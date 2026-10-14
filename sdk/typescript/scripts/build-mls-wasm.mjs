// Build the MLS WebAssembly module the TypeScript MessagingClient loads:
// compile crates/udb-mls-wasm for wasm32-unknown-unknown, then generate the
// Node bindings with wasm-bindgen into sdk/typescript/mls-wasm/.
//
// Needs: a Rust toolchain with the wasm32-unknown-unknown target, and the
// wasm-bindgen CLI at EXACTLY the `wasm-bindgen` crate version in Cargo.lock
// (checked below; a mismatched CLI produces bindings that fail at load time).
// `CARGO_TARGET_DIR` is honoured.
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url)); // sdk/typescript/scripts
const pkg = path.resolve(here, ".."); // sdk/typescript
const repo = path.resolve(pkg, "..", ".."); // repo root
const out = path.join(pkg, "mls-wasm");

function lockedVersion(name) {
  const lock = fs.readFileSync(path.join(repo, "Cargo.lock"), "utf8");
  const match = lock.match(new RegExp(`\\[\\[package\\]\\]\\r?\\nname = "${name}"\\r?\\nversion = "([^"]+)"`));
  if (!match) throw new Error(`build-mls-wasm: ${name} is not in Cargo.lock`);
  return match[1];
}

const wanted = lockedVersion("wasm-bindgen");
let cli;
try {
  cli = execFileSync("wasm-bindgen", ["--version"], { encoding: "utf8" }).trim();
} catch {
  console.error(
    `build-mls-wasm: the wasm-bindgen CLI is not installed; install version ${wanted} ` +
      `(cargo install wasm-bindgen-cli --version ${wanted} --locked, or the release binary).`,
  );
  process.exit(1);
}
if (cli !== `wasm-bindgen ${wanted}`) {
  console.error(`build-mls-wasm: found "${cli}", but Cargo.lock pins wasm-bindgen ${wanted}.`);
  process.exit(1);
}

// getrandom 0.3 (OpenMLS's `js` feature) takes its JS randomness backend from
// this cfg; getrandom 0.2 takes it from its `js` feature (crate manifest).
const rustflags = [process.env.RUSTFLAGS, '--cfg getrandom_backend="wasm_js"'].filter(Boolean).join(" ");
execFileSync(
  "cargo",
  ["build", "--locked", "-p", "udb-mls-wasm", "--release", "--target", "wasm32-unknown-unknown"],
  { cwd: repo, stdio: "inherit", env: { ...process.env, RUSTFLAGS: rustflags } },
);

const targetDir = process.env.CARGO_TARGET_DIR
  ? path.resolve(repo, process.env.CARGO_TARGET_DIR)
  : path.join(repo, "target");
const wasm = path.join(targetDir, "wasm32-unknown-unknown", "release", "udb_mls_wasm.wasm");
fs.rmSync(out, { recursive: true, force: true });
execFileSync("wasm-bindgen", ["--target", "nodejs", "--out-dir", out, wasm], { stdio: "inherit" });
console.log(`build-mls-wasm: wrote ${path.relative(repo, out)}`);
