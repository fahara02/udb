#!/usr/bin/env node
// UDB version-consistency checker / propagator.
//
//   node scripts/check-versions.mjs          # verify every manifest + protocol
//                                            # constant matches versions.json
//   node scripts/check-versions.mjs --fix    # rewrite manifests from versions.json
//   node scripts/check-versions.mjs --json    # machine-readable report
//
// `versions.json` is the single source of truth. UDB releases one version for
// the crate and every SDK; the wire PROTOCOL version is a separate compatibility
// number that MUST be identical everywhere. CI gates on the check; release
// workflows additionally assert tag == manifest == this.
// See VERSIONING.md.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = process.argv.includes("--fix");
const JSON_OUT = process.argv.includes("--json");

const spec = JSON.parse(fs.readFileSync(path.join(ROOT, "versions.json"), "utf8"));
const PROTOCOL = spec.protocol;
const C = spec.components;

const results = [];
function record(name, file, expected, actual, ok, note = "") {
  results.push({ name, file, expected, actual, ok, note });
}

function readFile(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}
function writeFile(rel, content) {
  fs.writeFileSync(path.join(ROOT, rel), content);
}
function fileExists(rel) {
  return fs.existsSync(path.join(ROOT, rel));
}

// A target whose value sits in capture group 2 of a /(prefix)(value)(suffix)/ regex.
function processTriple(name, file, expected, re, { snapshotOk = false } = {}) {
  if (!fileExists(file)) {
    record(name, file, expected, "(file missing)", false);
    return;
  }
  const content = readFile(file);
  const m = content.match(re);
  if (!m) {
    record(name, file, expected, "(pattern not found)", false);
    return;
  }
  const actual = m[2];
  const ok = actual === expected || (snapshotOk && actual === `${expected}-SNAPSHOT`);
  if (FIX && !ok) {
    // Preserve a -SNAPSHOT suffix on Maven dev versions when fixing.
    const replacement = snapshotOk && actual.endsWith("-SNAPSHOT") ? `${expected}-SNAPSHOT` : expected;
    writeFile(file, content.replace(re, `$1${replacement}$3`));
    record(name, file, expected, `${actual} → ${replacement}`, true, "fixed");
  } else {
    record(name, file, expected, actual, ok);
  }
}

// ── Component manifest versions ──────────────────────────────────────────────
processTriple("udb (crate)", "Cargo.toml", C.udb.version, /(^version = ")([^"]*)(")/m);
processTriple("sdk-python", "sdk/python/pyproject.toml", C["sdk-python"].version, /(\[project\][\s\S]*?\nversion = ")([^"]*)(")/);
processTriple(
  "sdk-python lock",
  "sdk/python/uv.lock",
  C["sdk-python"].version,
  /(\[\[package\]\]\s*\nname = "udb-client"\s*\nversion = ")([^"]*)(")/,
);
processTriple("sdk-typescript", "sdk/typescript/package.json", C["sdk-typescript"].version, /("version":\s*")([^"]*)(")/);
processTriple("sdk-csharp", "sdk/csharp/Udb.Client/Udb.Client.csproj", C["sdk-csharp"].version, /(<Version>)([^<]*)(<\/Version>)/);
processTriple("sdk-java", "sdk/java/pom.xml", C["sdk-java"].version, /(<version>)([^<]*)(<\/version>)/, { snapshotOk: true });
processTriple(
  "openapi",
  "api/udb-broker.swagger.json",
  C.udb.version,
  /("info":\s*\{\s*\n\s*"title":\s*"[^"]*",\s*\n\s*"version":\s*")([^"]*)(")/,
);

// go / php are git-tag-driven (no manifest version field). Their release version
// still follows the main UDB version; here we only note the intended version.
record("sdk-go", "(module tag sdk/go/v…)", C["sdk-go"].version, C["sdk-go"].version, true, "tag-driven");
record("sdk-php", "(release tag v… → Packagist)", C["sdk-php"].version, C["sdk-php"].version, true, "tag-driven");

// ── Protocol version (must be identical everywhere) ──────────────────────────
// Plain-text marker file.
{
  const file = "sdk/UDB_PROTOCOL_VERSION";
  const actual = fileExists(file) ? readFile(file).trim() : "(file missing)";
  const ok = actual === PROTOCOL;
  if (FIX && !ok && fileExists(file)) {
    writeFile(file, `${PROTOCOL}\n`);
    record("protocol marker", file, PROTOCOL, `${actual} → ${PROTOCOL}`, true, "fixed");
  } else {
    record("protocol marker", file, PROTOCOL, actual, ok);
  }
}
// Hardcoded constants in each SDK client.
const protoConsts = [
  ["protocol go", "sdk/go/udbclient/client.go", /(ProtocolVersion = ")([^"]*)(")/],
  ["protocol python", "sdk/python/udb_client/metadata.py", /(UDB_PROTOCOL_VERSION = ")([^"]*)(")/],
  ["protocol typescript", "sdk/typescript/client.ts", /(UDB_PROTOCOL_VERSION = ")([^"]*)(")/],
  ["protocol csharp", "sdk/csharp/Udb.Client/UdbClient.cs", /(ProtocolVersion = ")([^"]*)(")/],
  ["protocol java", "sdk/java/src/main/java/dev/udb/client/UdbClient.java", /(PROTOCOL_VERSION = ")([^"]*)(")/],
  ["protocol php (metadata)", "sdk/php/src/UdbMetadata.php", /(client_catalog_version'\]\s*\?\?\s*')([^']*)(')/],
  ["protocol php (config)", "sdk/php/config/udb.php", /(UDB_CLIENT_CATALOG_VERSION',\s*')([^']*)(')/],
];
for (const [name, file, re] of protoConsts) {
  processTriple(name, file, PROTOCOL, re);
}

// ── Report ───────────────────────────────────────────────────────────────────
const failures = results.filter((r) => !r.ok);

if (JSON_OUT) {
  console.log(JSON.stringify({ protocol: PROTOCOL, ok: failures.length === 0, results }, null, 2));
} else {
  const pad = (s, n) => String(s).padEnd(n);
  console.log(`UDB version check  (protocol ${PROTOCOL})  ${FIX ? "[--fix]" : ""}`);
  console.log("─".repeat(92));
  console.log(`${pad("component", 24)}${pad("expected", 14)}${pad("actual", 22)}status  file`);
  console.log("─".repeat(92));
  for (const r of results) {
    const status = r.note === "fixed" ? "FIXED " : r.ok ? "ok    " : "FAIL  ";
    console.log(`${pad(r.name, 24)}${pad(r.expected, 14)}${pad(r.actual, 22)}${status}${r.file}`);
  }
  console.log("─".repeat(92));
}

if (!FIX && failures.length > 0) {
  console.error(
    `\n✖ ${failures.length} version mismatch(es). Edit versions.json then run ` +
      `\`node scripts/check-versions.mjs --fix\` to propagate.`,
  );
  process.exit(1);
}
if (FIX) {
  const fixed = results.filter((r) => r.note === "fixed").length;
  console.log(`\n✔ versions.json propagated (${fixed} file(s) updated).`);
} else {
  console.log("\n✔ all versions consistent.");
}
