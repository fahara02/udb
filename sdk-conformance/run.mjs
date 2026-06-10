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
import { existsSync } from "node:fs";
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
    build: { cmd: "npx", args: ["tsc", "-p", "tsconfig.test.json"] },
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

const explicitTargets = process.argv.slice(2).filter((a) => LANGS[a]);
const selected = explicitTargets;
const targets = selected.length ? selected : Object.keys(LANGS);
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
