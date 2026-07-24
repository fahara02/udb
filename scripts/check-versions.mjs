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
  "example native java",
  "examples/native-services/java/pom.xml",
  C["sdk-java"].version,
  /(<artifactId>udb-native-services-java<\/artifactId>\s*\n\s*<version>)([^<]*)(<\/version>)/,
  { snapshotOk: true },
);
processTriple(
  "example native java sdk",
  "examples/native-services/java/pom.xml",
  C["sdk-java"].version,
  /(<artifactId>udb-java-client<\/artifactId>\s*\n\s*<version>)([^<]*)(<\/version>)/,
  { snapshotOk: true },
);
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

// Like processTriple but replaces ALL matches of the pattern (global replace).
function processAll(name, file, expected, re) {
  if (!fileExists(file)) {
    record(name, file, expected, "(file missing)", false);
    return;
  }
  const content = readFile(file);
  const gre = new RegExp(re.source, re.flags.includes("g") ? re.flags : re.flags + "g");
  const matches = [...content.matchAll(gre)];
  if (matches.length === 0) {
    record(name, file, expected, "(pattern not found)", false);
    return;
  }
  const actuals = [...new Set(matches.map((m) => m[2]))];
  const ok = actuals.every((a) => a === expected);
  if (FIX && !ok) {
    writeFile(file, content.replace(gre, `$1${expected}$3`));
    record(name, file, expected, `${actuals.join(",")} → ${expected}`, true, "fixed");
  } else {
    record(name, file, expected, actuals.length === 1 ? actuals[0] : actuals.join(","), ok);
  }
}

function processTripleOptional(name, file, expected, re, options = {}) {
  if (!fileExists(file)) {
    record(name, file, expected, "(optional file missing)", true, "optional");
    return;
  }
  processTriple(name, file, expected, re, options);
}

function processAllOptional(name, file, expected, re) {
  if (!fileExists(file)) {
    record(name, file, expected, "(optional file missing)", true, "optional");
    return;
  }
  processAll(name, file, expected, re);
}

// ── Brand sub-headers (HTML <sub> line in README / top-of-doc headers) ───────
// Pattern: crate v0.x.y | protocol v1.0.0</sub>
const subHeaderFiles = [
  ["README.md", "readme"],
  ["TESTING.md", "testing"],
  ["sdk-conformance/README.md", "sdk-conformance"],
  ["sdk/go/README.md", "sdk-go readme"],
  ["sdk/python/README.md", "sdk-python readme"],
  ["sdk/typescript/README.md", "sdk-typescript readme"],
  ["sdk/java/README.md", "sdk-java readme"],
  ["sdk/csharp/README.md", "sdk-csharp readme"],
  ["sdk/php/README.md", "sdk-php readme"],
];
for (const [file, label] of subHeaderFiles) {
  processTriple(`brand header (${label})`, file, C.udb.version,
    /(crate v)(\d+\.\d+\.\d+)( \| protocol v[\d.]+<\/sub>)/);
}

// ── ASCII-art box headers in docs/ and sdk/ ───────────────────────────────────
// Pattern: │    crate v0.x.y | protocol v1.0.0
const asciiHeaderFiles = [
  ["docs/annotations.md", "docs-annotations"],
  ["docs/architecture.md", "docs-architecture"],
  ["docs/integration.md", "docs-integration"],
  ["docs/security.md", "docs-security"],
  ["docs/testing.md", "docs-testing"],
  ["docs/native-services.md", "docs-native-services"],
  ["docs/operations.md", "docs-operations"],
  ["docs/README.md", "docs-readme"],
  ["docs/assets/udb_markdown_logo.md", "docs-logo"],
  ["sdk/README.md", "sdk-readme"],
];
for (const [file, label] of asciiHeaderFiles) {
  processTriple(`ascii header (${label})`, file, C.udb.version,
    /(│    crate v)(\d+\.\d+\.\d+)( \| protocol v[\d.]+)/);
}

// ── Additional prose / badge version refs ─────────────────────────────────────
processTriple("docs-logo badge line", "docs/assets/udb_markdown_logo.md", C.udb.version,
  /(UDB \| crate v)(\d+\.\d+\.\d+)( \| protocol v[\d.]+ \| policy-aware)/);
processTriple("docs-native-services version", "docs/native-services.md", C.udb.version,
  /(UDB )(\d+\.\d+\.\d+)( includes a native control plane)/);
processTriple("docs-readme intro", "docs/README.md", C.udb.version,
  /(public documentation for UDB )(\d+\.\d+\.\d+)(\. The guides)/);
processTriple("readme release row", "README.md", C.udb.version,
  /(crate\/SDK version `)(\d+\.\d+\.\d+)(`, wire protocol)/);
processTriple("sdk-readme current release", "sdk/README.md", C.udb.version,
  /(Current SDK release: `)(\d+\.\d+\.\d+)(`)/);
processTriple("sdk-readme generated code comment", "sdk/README.md", C.udb.version,
  /(crate\/package version `)(\d+\.\d+\.\d+)(`;)/);
processTriple("ops bench label", "docs/operations.md", C.udb.version,
  /(bench_snapshot\.py --label "release-)(\d+\.\d+\.\d+)(")/);
processAll("versioning release refs", "VERSIONING.md", C.udb.version,
  /(^|[^0-9]v?)(0\.\d+\.\d+)([^0-9]|$)/gm);

// ── docs/site (GitHub Pages) version refs ─────────────────────────────────────
// The published site drifted all the way back to 0.3.6 because it was NOT
// governed here — every page footer + the index pill track the crate version,
// and the sdks install table tracks each SDK. Gating them makes
// `--fix` bump the site on every release instead of shipping a stale page.
for (const page of ["index", "architecture", "control-plane", "data-plane",
  "enterprise", "security", "sdks"]) {
  processAllOptional(`site ${page} footer`, `docs/site/${page}.html`, C.udb.version,
    /(crate v)(\d+\.\d+\.\d+)( · protocol)/);
}
processAllOptional("site index pill", "docs/site/index.html", C.udb.version,
  /(<b>v)(\d+\.\d+\.\d+)(<\/b> · 18 backends)/);
processAllOptional("site sdks go install", "docs/site/sdks.html", C["sdk-go"].version,
  /(sdk\/go@v)(\d+\.\d+\.\d+)(<)/);
processAllOptional("site sdks python install", "docs/site/sdks.html", C["sdk-python"].version,
  /(udb-client==)(\d+\.\d+\.\d+)(<)/);
processAllOptional("site sdks ts install", "docs/site/sdks.html", C["sdk-typescript"].version,
  /(@udb_plus\/sdk@)(\d+\.\d+\.\d+)(<)/);
processAllOptional("site sdks php install", "docs/site/sdks.html", C["sdk-php"].version,
  /(udb-laravel:\^)(\d+\.\d+\.\d+)(<)/);
processAllOptional("site sdks csharp install", "docs/site/sdks.html", C["sdk-csharp"].version,
  /(Udb\.Client --version )(\d+\.\d+\.\d+)(<)/);

// ── README.md install table ───────────────────────────────────────────────────
processTriple("readme install go", "README.md", C["sdk-go"].version,
  /(go get github\.com\/fahara02\/udb\/sdk\/go@v)(\d+\.\d+\.\d+)(` \|)/);
processTriple("readme install python", "README.md", C["sdk-python"].version,
  /(pip install udb-client==)(\d+\.\d+\.\d+)(` \|)/);
processTriple("readme install typescript", "README.md", C["sdk-typescript"].version,
  /(npm i @udb_plus\/sdk@)(\d+\.\d+\.\d+)(` \|)/);
processTriple("readme install php", "README.md", C["sdk-php"].version,
  /(composer require fahara02\/udb-laravel:\^)(\d+\.\d+\.\d+)(` \|)/);
processTriple("readme install csharp", "README.md", C["sdk-csharp"].version,
  /(dotnet add package Udb\.Client --version )(\d+\.\d+\.\d+)(` \|)/);
processTriple("readme install java", "README.md", C["sdk-java"].version,
  /(`dev\.udb:udb-java-client` \(`)(\d+\.\d+\.\d+)(` target)/);

// ── sdk/README.md install table + PHP consumer block ─────────────────────────
processTriple("sdk-readme install go", "sdk/README.md", C["sdk-go"].version,
  /(go get github\.com\/fahara02\/udb\/sdk\/go@v)(\d+\.\d+\.\d+)(` \|)/);
processTriple("sdk-readme install python", "sdk/README.md", C["sdk-python"].version,
  /(pip install udb-client==)(\d+\.\d+\.\d+)(` \|)/);
processTriple("sdk-readme install typescript", "sdk/README.md", C["sdk-typescript"].version,
  /(npm i @udb_plus\/sdk@)(\d+\.\d+\.\d+)(` \|)/);
processTriple("sdk-readme install php", "sdk/README.md", C["sdk-php"].version,
  /(composer require fahara02\/udb-laravel:\^)(\d+\.\d+\.\d+)(` \|)/);
processTriple("sdk-readme install csharp", "sdk/README.md", C["sdk-csharp"].version,
  /(dotnet add package Udb\.Client --version )(\d+\.\d+\.\d+)(` \|)/);
processTriple("sdk-readme php consumer block", "sdk/README.md", C["sdk-php"].version,
  /(^composer require fahara02\/udb-laravel:\^)(\d+\.\d+\.\d+)(\n)/m);

// ── Per-SDK README install commands ───────────────────────────────────────────
processTriple("sdk-go install get", "sdk/go/README.md", C["sdk-go"].version,
  /(go get github\.com\/fahara02\/udb\/sdk\/go@v)(\d+\.\d+\.\d+)(\n)/);
processTriple("sdk-go install cli", "sdk/go/README.md", C["sdk-go"].version,
  /(go install github\.com\/fahara02\/udb\/sdk\/go\/cmd\/udb@v)(\d+\.\d+\.\d+)(\n)/);
processTriple("sdk-python install", "sdk/python/README.md", C["sdk-python"].version,
  /(pip install udb-client==)(\d+\.\d+\.\d+)(\n)/);
processTriple("sdk-python pydantic install", "sdk/python/README.md", C["sdk-python"].version,
  /(pip install "udb-client\[pydantic\]==)(\d+\.\d+\.\d+)(")/);
processTriple("sdk-typescript install", "sdk/typescript/README.md", C["sdk-typescript"].version,
  /(npm i @udb_plus\/sdk@)(\d+\.\d+\.\d+)(\n)/);
processTriple("sdk-csharp install client", "sdk/csharp/README.md", C["sdk-csharp"].version,
  /(dotnet add package Udb\.Client --version )(\d+\.\d+\.\d+)(\n)/);
processTriple("sdk-csharp install cli", "sdk/csharp/README.md", C["sdk-csharp"].version,
  /(dotnet tool install --global Udb\.Cli --version )(\d+\.\d+\.\d+)(\n)/);
processTriple("sdk-java readme manifest", "sdk/java/README.md", C["sdk-java"].version,
  /(Current manifest version: `)(\d+\.\d+\.\d+)(-SNAPSHOT`)/);
processTriple("sdk-java readme target", "sdk/java/README.md", C["sdk-java"].version,
  /(Release target: `)(\d+\.\d+\.\d+)(`)/);
processTriple("sdk-php install list item", "sdk/php/README.md", C["sdk-php"].version,
  /(- `composer require fahara02\/udb-laravel:\^)(\d+\.\d+\.\d+)(`)/);
processTriple("sdk-php install block", "sdk/php/README.md", C["sdk-php"].version,
  /(^composer require fahara02\/udb-laravel:\^)(\d+\.\d+\.\d+)(\n)/m);

// ── Generated artefacts and binaries ─────────────────────────────────────────
processTriple("ts generated client comment", "sdk/typescript/generatedClient.ts", C["sdk-typescript"].version,
  /(\/\/ UDB v)(\d+\.\d+\.\d+)( · protocol v[\d.]+ · \d+ service\(s\))/);
processTriple("ts generated client version", "sdk/typescript/generatedClient.ts", C["sdk-typescript"].version,
  /(export const UDB_SDK_VERSION = ")(\d+\.\d+\.\d+)(")/);
processTripleOptional("ts dist generated client comment", "sdk/typescript/dist/generatedClient.js", C["sdk-typescript"].version,
  /(\/\/ UDB v)(\d+\.\d+\.\d+)( · protocol v[\d.]+ · \d+ service\(s\))/);
processTripleOptional("ts dist generated client version", "sdk/typescript/dist/generatedClient.js", C["sdk-typescript"].version,
  /(exports\.UDB_SDK_VERSION = ")(\d+\.\d+\.\d+)(")/);
processTripleOptional("ts dist generated declarations", "sdk/typescript/dist/generatedClient.d.ts", C["sdk-typescript"].version,
  /(UDB_SDK_VERSION = ")(\d+\.\d+\.\d+)(")/);
processTriple("ts dist-test generated client comment", "sdk/typescript/dist-test/generatedClient.js", C["sdk-typescript"].version,
  /(\/\/ UDB v)(\d+\.\d+\.\d+)( · protocol v[\d.]+ · \d+ service\(s\))/);
processTriple("ts dist-test generated client version", "sdk/typescript/dist-test/generatedClient.js", C["sdk-typescript"].version,
  /(exports\.UDB_SDK_VERSION = ")(\d+\.\d+\.\d+)(")/);
processTriple("ts bin launcher comment", "sdk/typescript/bin/udb.js", C["sdk-typescript"].version,
  /(bundled with @udb_plus\/sdk v)(\d+\.\d+\.\d+)(\.)/);
processTriple("ts bin udb version", "sdk/typescript/bin/udb.js", C["sdk-typescript"].version,
  /(const UDB_VERSION = ")(\d+\.\d+\.\d+)(")/);
processAll("ts bin launcher refs", "sdk/typescript/bin/udb.js", C["sdk-typescript"].version,
  /(^|[^0-9]v?)(0\.\d+\.\d+)([^0-9]|$)/gm);
processTriple("go generated client comment", "sdk/go/udbclient/generated_client.go", C["sdk-go"].version,
  /(\/\/ \(UDB v)(\d+\.\d+\.\d+)(, wire protocol [\d.]+\)\.)/);
processTriple("go generated client version", "sdk/go/udbclient/generated_client.go", C["sdk-go"].version,
  /(const SDKVersion = ")(\d+\.\d+\.\d+)(")/);
processAll("go cli launcher refs", "sdk/go/cmd/udb/main.go", C["sdk-go"].version,
  /(^|[^0-9]v?)(0\.\d+\.\d+)([^0-9]|$)/gm);
processTriple("python generated client version", "sdk/python/udb_client/generated_client.py", C["sdk-python"].version,
  /(UDB version:\s+)(\d+\.\d+\.\d+)(\n)/);
processAll("python cli launcher refs", "sdk/python/udb_client/_cli.py", C["sdk-python"].version,
  /(^|[^0-9]v?)(0\.\d+\.\d+)([^0-9]|$)/gm);
processTripleOptional("python package metadata version", "sdk/python/udb_client.egg-info/PKG-INFO", C["sdk-python"].version,
  /(^Version: )(\d+\.\d+\.\d+)(\r?\n)/m);
processTripleOptional("python package metadata header", "sdk/python/udb_client.egg-info/PKG-INFO", C["sdk-python"].version,
  /(crate v)(\d+\.\d+\.\d+)( \| protocol v[\d.]+<\/sub>)/);
processTripleOptional("python package metadata install", "sdk/python/udb_client.egg-info/PKG-INFO", C["sdk-python"].version,
  /(pip install udb-client==)(\d+\.\d+\.\d+)(\r?\n)/);
processTripleOptional("python package metadata pydantic install", "sdk/python/udb_client.egg-info/PKG-INFO", C["sdk-python"].version,
  /(pip install "udb-client\[pydantic\]==)(\d+\.\d+\.\d+)(")/);
processTriple("csharp generated client version", "sdk/csharp/Udb.Client/GeneratedClient.cs", C["sdk-csharp"].version,
  /(\/\/   UDB version:\s+)(\d+\.\d+\.\d+)(\n)/);
processAll("csharp cli launcher refs", "sdk/csharp/Udb.Cli/UdbCli.cs", C["sdk-csharp"].version,
  /(^|[^0-9]v?)(0\.\d+\.\d+)([^0-9]|$)/gm);
processAll("csharp cli manifest refs", "sdk/csharp/Udb.Cli/Udb.Cli.csproj", C["sdk-csharp"].version,
  /(^|[^0-9]v?)(0\.\d+\.\d+)([^0-9]|$)/gm);
processTriple("java generated client version", "sdk/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java", C["sdk-java"].version,
  /(\/\/ udb )(\d+\.\d+\.\d+)( · protocol [\d.]+ · \d+ services)/);
processAll("java cli launcher refs", "sdk/java/src/main/java/dev/udb/cli/Launcher.java", C["sdk-java"].version,
  /(^|[^0-9]v?)(0\.\d+\.\d+)([^0-9]|$)/gm);
processTriple("php generated client version", "sdk/php/src/Generated/GeneratedClient.php", C["sdk-php"].version,
  /(UDB version \...... )(\d+\.\d+\.\d+)(\n)/);
processAll("php cli launcher refs", "sdk/php/bin/udb", C["sdk-php"].version,
  /(^|[^0-9]v?)(0\.\d+\.\d+)([^0-9]|$)/gm);
// package-lock.json has two "version" entries for @udb_plus/sdk – update both.
processAll("ts package-lock", "sdk/typescript/package-lock.json", C["sdk-typescript"].version,
  /("name": "@udb_plus\/sdk",\n\s*"version": ")(\d+\.\d+\.\d+)(")/);
processTriple("cargo-lock udb", "Cargo.lock", C.udb.version,
  /(name = "udb"\nversion = ")(\d+\.\d+\.\d+)(")/);
processTriple("native contract json", "docs/generated/udb-native-contract.json", C.udb.version,
  /("udb_version": ")(\d+\.\d+\.\d+)(")/);

// ── CI workflow descriptions ──────────────────────────────────────────────────
processTriple("ci release-csharp description", ".github/workflows/release-csharp-sdk.yml", C.udb.version,
  /(Expected UDB release version, for example )(\d+\.\d+\.\d+)(")/);

// ── SDK launcher templates (example text) ─────────────────────────────────────
processTriple("java tmpl version example", "sdk-templates/java/src/main/java/dev/udb/cli/Launcher.java.tmpl", C.udb.version,
  /(prints e\.g\. "udb )(\d+\.\d+\.\d+)("; accept)/);


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

// W1 (0.4.23): fail closed when CHANGELOG.md lacks a non-empty section for the
// version under release - a release page must explain itself. Same posture as
// the version guard; --fix does not write prose for you.
if (!FIX) {
  const changelog = fs
    .readFileSync(path.join(ROOT, "CHANGELOG.md"), "utf8")
    .replaceAll("\r\n", "\n");
  const version = spec.components.udb.version;
  const escaped = version.replace(/[.]/g, "\\.");
  const heading = new RegExp("^## \\[?" + escaped + "\\]?[^\n]*$", "m");
  const match = heading.exec(changelog);
  let hasNotes = false;
  if (match) {
    const rest = changelog.slice(match.index + match[0].length);
    const next = rest.search(/^## /m);
    hasNotes = (next === -1 ? rest : rest.slice(0, next)).trim().length > 0;
  }
  if (!hasNotes) {
    failures.push({
      file: "CHANGELOG.md",
      note: "missing or empty section for " + version,
    });
    console.error("CHANGELOG.md: missing or empty section for " + version);
  }
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
