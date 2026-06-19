#!/usr/bin/env node
// UDB SDK cross-language conformance runner (plan M9).
//
// Drives the per-language conformance suites that assert the SAME contract
// across TypeScript, Python, Go, Java, C#, and PHP — metadata/header parity,
// `requested_scopes` population, AuthzCache TTL behavior, and policy-bundle
// signature verification — then prints a parity matrix.
//
// These suites are UNIT-level (no broker required): they build outbound
// requests with capturing fakes and assert the wire shape. Live, broker-backed
// cases (login/refresh single-flight, can/require end-to-end, native-access DSN
// redaction) are listed in README.md and run separately against a dev target.
//
// Usage:
//   node sdk-conformance/run.mjs            # run every available language
//   node sdk-conformance/run.mjs go python  # run a subset
//
// In default local mode, a language whose toolchain or dependencies are missing
// is reported SKIP, not FAIL. Explicitly named languages run in strict mode so
// CI can make every supported SDK a hard gate.

import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const sdk = (p) => join(repoRoot, "sdk", p);

// Each language maps to: how to detect its toolchain, an optional setup step,
// the test command, and a regex that recognizes a PASS in its output.
const LANGS = {
  typescript: {
    cwd: sdk("typescript"),
    detect: () => has("node"),
    setup: { cmd: "npm", args: ["install", "--no-audit", "--no-fund", "--silent"] },
    build: {
      cmd: "npm",
      args: ["run", "--silent", "bundle-proto", "&&", "npx", "tsc", "-p", "tsconfig.test.json"],
    },
    test: {
      cmd: "node",
      args: [
        "--test",
        "dist-test/facade.test.js",
        "dist-test/live-auth.test.js",
        "dist-test/negotiation.test.js",
        "dist-test/outbound.test.js",
        "dist-test/refresh.test.js",
      ],
    },
    pass: /\bpass\s+\d+/i,
    fail: /\bfail\s+[1-9]/i,
  },
  python: {
    cwd: sdk("python"),
    detect: () => has(venvPython()) || has("python"),
    test: { cmd: venvPython() || "python", args: ["-m", "pytest", "-q", "tests"] },
    pass: /\d+\s+passed/,
    fail: /\d+\s+failed|error/i,
  },
  go: {
    cwd: sdk("go"),
    detect: () => has("go", ["version"]),
    test: { cmd: "go", args: ["test", "./udbclient/..."] },
    pass: /(^|\n)ok\s|\bPASS\b/,
    fail: /(^|\n)FAIL\b|---\s+FAIL/,
  },
  csharp: {
    cwd: sdk("csharp"),
    detect: () => has("dotnet") && existsSync(join(sdk("csharp"), "Udb.Client.Tests")),
    test: { cmd: "dotnet", args: ["test", "Udb.Client.Tests", "--nologo", "-v", "q"] },
    pass: /Passed!\s+-\s+Failed:\s+0/,
    fail: /Failed!\s|Failed:\s+[1-9]/,
  },
  java: {
    cwd: sdk("java"),
    // CI runs JUnit via Maven; Maven is frequently absent locally.
    detect: () => has("mvn"),
    test: { cmd: "mvn", args: ["-B", "-ntp", "test"] },
    pass: /BUILD SUCCESS/,
    fail: /BUILD FAILURE|Tests run:.*Failures: [1-9]/,
    skipReason: "requires Maven (mvn) — runs in CI",
  },
  php: {
    cwd: sdk("php"),
    // Pest needs vendor/; the generated stubs need ext-grpc/ext-protobuf, which
    // are commonly absent locally. CI installs them.
    detect: () => has("php") && existsSync(join(sdk("php"), "vendor", "bin")),
    test: { cmd: php(), args: ["vendor/bin/pest", "tests/Unit"] },
    pass: /Tests:\s+.*\b\d+\s+passed/,
    fail: /failed|errors/i,
    skipReason: "requires composer install + ext-grpc/ext-protobuf — runs in CI",
  },
  metadata: {
    cwd: repoRoot,
    detect: () => true,
    check: checkAliasMetadata,
  },
};

function has(bin, args = ["--version"]) {
  if (!bin) return false;
  const probe = spawnSync(bin, args, { stdio: "ignore", shell: true });
  return probe.status === 0;
}
function venvPython() {
  const win = join(sdk("python"), ".venv", "Scripts", "python.exe");
  const nix = join(sdk("python"), ".venv", "bin", "python");
  return existsSync(win) ? win : existsSync(nix) ? nix : null;
}
function php() {
  return process.platform === "win32" ? "php" : "php";
}

function run(step, cwd) {
  const r = spawnSync(step.cmd, step.args, { cwd, encoding: "utf8", shell: true });
  return { code: r.status, out: `${r.stdout || ""}${r.stderr || ""}` };
}

function readJson(rel) {
  return JSON.parse(readFileSync(join(repoRoot, rel), "utf8"));
}

function eachSwaggerOperation(swagger, fn) {
  for (const [path, methods] of Object.entries(swagger.paths || {})) {
    for (const [method, op] of Object.entries(methods || {})) {
      if (!op || typeof op !== "object") continue;
      fn(path, method, op);
    }
  }
}

function checkAliasMetadata() {
  const failures = [];
  const swaggerPath = join(repoRoot, "api", "udb-broker.swagger.json");
  if (!existsSync(swaggerPath)) {
    return { status: "SKIP", note: "api/udb-broker.swagger.json missing" };
  }
  const swagger = readJson("api/udb-broker.swagger.json");
  let operations = 0;
  let aliasExtensions = 0;
  eachSwaggerOperation(swagger, (path, method, op) => {
    operations += 1;
    const operationId = String(op.operationId || "");
    const alias = String(op["x-udb-sdk-alias"] || "");
    if (!operationId) failures.push(`${method.toUpperCase()} ${path}: missing operationId`);
    if (/^[A-Za-z]+Service_[A-Za-z0-9]+$/.test(operationId)) {
      failures.push(`${method.toUpperCase()} ${path}: generated operationId ${operationId}`);
    }
    if (!alias) failures.push(`${method.toUpperCase()} ${path}: missing x-udb-sdk-alias`);
    if (alias) aliasExtensions += 1;
  });
  if (operations === 0) failures.push("Swagger contains no operations");

  const templateChecks = [
    ["sdk-templates/typescript/generatedClient.ts.tmpl", "{{RPC_ALIAS_CAMEL}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/python/udb_client/generated_client.py.tmpl", "{{RPC_ALIAS_SNAKE}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/php/src/Generated/GeneratedClient.php.tmpl", "{{RPC_ALIAS_CAMEL}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl", "{{RPC_ALIAS_PASCAL}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/csharp/Udb.Client/GeneratedClient.cs.tmpl", "{{RPC_ALIAS_PASCAL}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/go/udbclient/generated_client.go.tmpl", "{{RPC_ALIAS_SNAKE}}", "{{REST_OPERATION_ID}}"],
  ];
  for (const [rel, aliasNeedle, wireNeedle] of templateChecks) {
    const text = readFileSync(join(repoRoot, rel), "utf8");
    if (!text.includes(aliasNeedle)) failures.push(`${rel}: missing ${aliasNeedle}`);
    if (!text.includes(wireNeedle)) failures.push(`${rel}: missing ${wireNeedle}`);
  }

  if (failures.length) {
    return { status: "FAIL", note: failures.slice(0, 8).join("; ") };
  }
  return { status: "PASS", note: `${operations} Swagger operations, ${aliasExtensions} SDK aliases` };
}

const explicitTargets = process.argv.slice(2).filter((a) => LANGS[a]);
const selected = explicitTargets;
const defaultTargets = Object.keys(LANGS).filter((name) => name !== "metadata");
const targets = selected.length ? selected : defaultTargets;
const strictSelected = selected.length > 0;

const results = [];
for (const name of targets) {
  const cfg = LANGS[name];
  if (!existsSync(cfg.cwd)) {
    results.push({ name, status: strictSelected ? "FAIL" : "SKIP", note: "sdk dir missing" });
    continue;
  }
  if (!cfg.detect()) {
    results.push({
      name,
      status: strictSelected ? "FAIL" : "SKIP",
      note: cfg.skipReason || "toolchain not found",
    });
    continue;
  }
  process.stderr.write(`\n=== ${name} ===\n`);
  if (cfg.check) {
    results.push({ name, ...cfg.check() });
    continue;
  }
  if (cfg.setup) run(cfg.setup, cfg.cwd);
  if (cfg.build) {
    const b = run(cfg.build, cfg.cwd);
    if (b.code !== 0) { results.push({ name, status: "FAIL", note: "build/compile error" }); process.stderr.write(b.out); continue; }
  }
  const t = run(cfg.test, cfg.cwd);
  const passed = cfg.pass.test(t.out) && !(cfg.fail && cfg.fail.test(t.out));
  results.push({ name, status: passed && t.code === 0 ? "PASS" : "FAIL", note: passed ? "" : `exit ${t.code}` });
  if (!passed) process.stderr.write(t.out);
}

// Parity matrix
process.stdout.write("\nUDB SDK conformance parity\n");
process.stdout.write("--------------------------\n");
let failed = 0;
for (const r of results) {
  if (r.status === "FAIL") failed++;
  const icon = r.status === "PASS" ? "PASS" : r.status === "SKIP" ? "skip" : "FAIL";
  process.stdout.write(`  ${r.name.padEnd(12)} ${icon}${r.note ? "  (" + r.note + ")" : ""}\n`);
}
process.stdout.write("--------------------------\n");
process.exit(failed > 0 ? 1 : 0);
