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
//   node sdk-conformance/run.mjs                         # run every available language
//   node sdk-conformance/run.mjs go python               # run a subset
//   node sdk-conformance/run.mjs metadata error-details  # run focused contract gates
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
    detect: () => Boolean(pythonWithPytest()),
    test: { cmd: pythonWithPytest() || "python", args: ["-m", "pytest", "-q", "tests"] },
    pass: /\d+\s+passed/,
    fail: /\d+\s+failed|error/i,
    skipReason: "requires pytest in sdk/python/.venv or system Python",
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

const PSEUDO_TARGETS = {
  "error-details": {
    cwd: repoRoot,
    detect: () => true,
    check: checkErrorDetailConformance,
  },
};

const TARGETS = { ...LANGS, ...PSEUDO_TARGETS };

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
function pythonWithPytest() {
  for (const candidate of [venvPython(), "python"].filter(Boolean)) {
    if (has(candidate, ["-m", "pytest", "--version"])) return candidate;
  }
  return null;
}
function php() {
  return process.platform === "win32" ? "php" : "php";
}

function run(step, cwd) {
  const r = spawnSync(step.cmd, step.args, { cwd, encoding: "utf8", shell: true });
  return { code: r.status, out: `${r.stdout || ""}${r.stderr || ""}` };
}

function runnable(name, cfg, strict = strictSelected) {
  if (!existsSync(cfg.cwd)) {
    return { ok: false, status: strict ? "FAIL" : "SKIP", note: "sdk dir missing" };
  }
  if (!cfg.detect()) {
    return {
      ok: false,
      status: strict ? "FAIL" : "SKIP",
      note: cfg.skipReason || "toolchain not found",
    };
  }
  return { ok: true, status: "PASS", note: "" };
}

function runLanguageSlice(name, cfg, setup, build, test) {
  const ready = runnable(name, cfg, Boolean(process.env.CI) || strictSelected);
  if (!ready.ok) return { name, status: ready.status, note: ready.note };
  process.stderr.write(`\n=== error-details:${name} ===\n`);
  if (setup) {
    const s = run(setup, cfg.cwd);
    if (s.code !== 0) {
      process.stderr.write(s.out);
      return { name, status: "FAIL", note: "setup error" };
    }
  }
  if (build) {
    const b = run(build, cfg.cwd);
    if (b.code !== 0) {
      process.stderr.write(b.out);
      return { name, status: "FAIL", note: "build/compile error" };
    }
  }
  const t = run(test, cfg.cwd);
  if (t.code !== 0) {
    process.stderr.write(t.out);
    return { name, status: "FAIL", note: `exit ${t.code}` };
  }
  return { name, status: "PASS", note: "" };
}

function checkErrorDetailConformance() {
  const slices = [
    runLanguageSlice(
      "typescript",
      LANGS.typescript,
      LANGS.typescript.setup,
      LANGS.typescript.build,
      { cmd: "node", args: ["--test", "dist-test/sdkhelpers.test.js"] },
    ),
    runLanguageSlice(
      "python",
      LANGS.python,
      null,
      null,
      {
        cmd: pythonWithPytest() || "python",
        args: ["-m", "pytest", "-q", "tests/test_simple_client.py", "-k", "error_detail"],
      },
    ),
    runLanguageSlice(
      "go",
      LANGS.go,
      null,
      null,
      { cmd: "go", args: ["test", "./udbclient", "-run", "TestErrorDetail", "-count=1"] },
    ),
    runLanguageSlice(
      "csharp",
      LANGS.csharp,
      null,
      null,
      {
        cmd: "dotnet",
        args: ["test", "Udb.Client.Tests", "--filter", "FullyQualifiedName~UdbRpcExceptionTests", "--nologo", "-v", "q"],
      },
    ),
    runLanguageSlice(
      "java",
      LANGS.java,
      null,
      null,
      { cmd: "mvn", args: ["-B", "-ntp", "-Dtest=UdbRpcExceptionTest", "test"] },
    ),
    runLanguageSlice(
      "php",
      LANGS.php,
      null,
      null,
      { cmd: php(), args: ["vendor/bin/pest", "tests/Unit/SimpleClientTest.php", "--filter", "errorDetail"] },
    ),
  ];
  const failures = slices.filter((r) => r.status === "FAIL");
  const skipped = slices.filter((r) => r.status === "SKIP");
  if (failures.length) {
    return {
      status: "FAIL",
      note: failures.map((r) => `${r.name}: ${r.note}`).join("; "),
    };
  }
  if (skipped.length && !strictSelected) {
    return {
      status: "SKIP",
      note: skipped.map((r) => `${r.name}: ${r.note}`).join("; "),
    };
  }
  if (skipped.length) {
    return {
      status: "PASS",
      note: `available SDK slices passed; skipped ${skipped.map((r) => r.name).join(", ")} (${skipped.map((r) => r.note).join("; ")})`,
    };
  }
  return {
    status: "PASS",
    note: "typed validation/quota/transport ErrorDetail + field violations aligned across SDK slices",
  };
}

function readJson(rel) {
  return JSON.parse(readFileSync(join(repoRoot, rel), "utf8"));
}

function readText(rel) {
  return readFileSync(join(repoRoot, rel), "utf8");
}

function eachSwaggerOperation(swagger, fn) {
  for (const [path, methods] of Object.entries(swagger.paths || {})) {
    for (const [method, op] of Object.entries(methods || {})) {
      if (!op || typeof op !== "object") continue;
      fn(path, method, op);
    }
  }
}

function normalizeHttpPath(path) {
  return String(path || "")
    .replace(/\{[^}]+\}/g, "{}")
    .replace(/\/+/g, "/");
}

function compareSwaggerRoutes(failures, expected, swagger) {
  const byOperationId = new Map();
  eachSwaggerOperation(swagger, (path, method, op) => {
    const operationId = String(op.operationId || "");
    if (!operationId) return;
    if (byOperationId.has(operationId)) {
      failures.push(`Swagger: duplicate operationId ${operationId}`);
      return;
    }
    byOperationId.set(operationId, {
      method: String(method || "").toLowerCase(),
      path: normalizeHttpPath(path),
      alias: String(op["x-udb-sdk-alias"] || ""),
    });
  });

  for (const info of expected.values()) {
    if (!info.httpMethod || !info.httpPath) continue;
    const got = byOperationId.get(info.operationId);
    if (!got) {
      failures.push(`Swagger: missing route for ${info.path} operationId ${info.operationId}`);
      continue;
    }
    const expectedMethod = String(info.httpMethod || "").toLowerCase();
    const expectedPath = normalizeHttpPath(info.httpPath);
    if (got.alias !== info.apiAlias) {
      failures.push(
        `Swagger: ${info.operationId} alias mismatch (${got.alias} != ${info.apiAlias})`,
      );
    }
    if (got.method !== expectedMethod || got.path !== expectedPath) {
      failures.push(
        `Swagger: ${info.operationId} route mismatch (${got.method} ${got.path} != ${expectedMethod} ${expectedPath})`,
      );
    }
  }
}

function objectBlock(text, name) {
  const start = text.indexOf(name);
  if (start < 0) throw new Error(`missing ${name}`);
  const assign = text.indexOf("=", start);
  if (assign < 0) throw new Error(`missing ${name} assignment`);
  const brace = text.indexOf("{", assign);
  const bracket = text.indexOf("[", assign);
  const candidates = [brace, bracket].filter((i) => i >= 0);
  const open = Math.min(...candidates);
  if (!Number.isFinite(open)) throw new Error(`missing ${name} object body`);
  const close = text[open] === "{" ? "}" : "]";
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === text[open]) depth += 1;
    if (ch === close) {
      depth -= 1;
      if (depth === 0) return text.slice(open + 1, i);
    }
  }
  throw new Error(`unterminated ${name} object body`);
}

function parseQuotedMap(text, name, entryRe = /"([^"]+)"\s*(?::|=>)\s*"([^"]*)"/g) {
  const body = objectBlock(text, name);
  const out = new Map();
  for (const match of body.matchAll(entryRe)) {
    out.set(match[1], match[2]);
  }
  if (out.size === 0) throw new Error(`${name} contains no entries`);
  return out;
}

function parseGoIdentities(text) {
  const out = new Map();
  const re = /\{Service:\s*"([^"]+)",\s*ServicePkg:\s*"([^"]+)",\s*FullMethod:\s*"([^"]+)",\s*Name:\s*"([^"]+)",\s*APIAlias:\s*"([^"]+)",\s*OperationID:\s*"([^"]+)",\s*HTTPMethod:\s*"([^"]*)",\s*HTTPPath:\s*"([^"]*)",[\s\S]*?OperationKind:\s*"([^"]+)"/g;
  for (const match of text.matchAll(re)) {
    out.set(match[3], {
      path: match[3],
      service: match[1],
      wireName: match[4],
      apiAlias: match[5],
      operationId: match[6],
      httpMethod: match[7],
      httpPath: match[8],
      operationKind: match[9],
    });
  }
  if (out.size === 0) throw new Error("Go AllRPCs contains no identity entries");
  return out;
}

function parseJavaIdentities(text) {
  const out = new Map();
  const re = /map\.put\("([^"]+)",\s*new RpcIdentity\("([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]*)",\s*"([^"]*)"\)\)/g;
  for (const match of text.matchAll(re)) {
    out.set(match[1], {
      path: match[2],
      service: match[3],
      wireName: match[4],
      apiAlias: match[5],
      operationId: match[6],
      operationKind: match[7],
      httpMethod: match[8],
      httpPath: match[9],
    });
  }
  if (out.size === 0) throw new Error("Java RPC_IDENTITIES contains no entries");
  return out;
}

function parseCsharpIdentities(text) {
  const out = new Map();
  const re = /map\["([^"]+)"\]\s*=\s*new RpcIdentity\("([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]*)",\s*"([^"]*)"\)/g;
  for (const match of text.matchAll(re)) {
    out.set(match[1], {
      path: match[2],
      service: match[3],
      wireName: match[4],
      apiAlias: match[5],
      operationId: match[6],
      operationKind: match[7],
      httpMethod: match[8],
      httpPath: match[9],
    });
  }
  if (out.size === 0) throw new Error("C# GeneratedRpcIdentities contains no entries");
  return out;
}

function phpKeyFor(info) {
  return `${info.service}/${info.wireName}`;
}

function compareMap(failures, language, expected, actual, pickKey = (info) => info.path) {
  for (const info of expected.values()) {
    const key = pickKey(info);
    const got = actual.get(key);
    if (!got) {
      failures.push(`${language}: missing identity for ${key}`);
      continue;
    }
    if (got.apiAlias !== info.apiAlias || got.operationId !== info.operationId) {
      failures.push(
        `${language}: ${key} alias/operationId mismatch (${got.apiAlias}/${got.operationId} != ${info.apiAlias}/${info.operationId})`,
      );
    }
    if (got.operationKind !== info.operationKind) {
      failures.push(`${language}: ${key} operationKind mismatch (${got.operationKind} != ${info.operationKind})`);
    }
    if (got.httpMethod !== info.httpMethod || got.httpPath !== info.httpPath) {
      failures.push(
        `${language}: ${key} HTTP metadata mismatch (${got.httpMethod} ${got.httpPath} != ${info.httpMethod} ${info.httpPath})`,
      );
    }
  }
  if (actual.size !== expected.size) {
    failures.push(`${language}: identity count ${actual.size} != Go ${expected.size}`);
  }
}

function compareFlatMaps(
  failures,
  language,
  expected,
  aliasMap,
  operationMap,
  operationKindMap,
  httpMethodMap,
  httpPathMap,
  pickKey = (info) => info.path,
) {
  const actual = new Map();
  for (const info of expected.values()) {
    const key = pickKey(info);
    actual.set(key, {
      apiAlias: aliasMap.get(key),
      operationId: operationMap.get(key),
      operationKind: operationKindMap.get(key),
      httpMethod: httpMethodMap.get(key),
      httpPath: httpPathMap.get(key),
    });
  }
  compareMap(failures, language, expected, actual, pickKey);
  if (aliasMap.size !== expected.size) {
    failures.push(`${language}: alias map count ${aliasMap.size} != Go ${expected.size}`);
  }
  if (operationMap.size !== expected.size) {
    failures.push(`${language}: operationId map count ${operationMap.size} != Go ${expected.size}`);
  }
  if (operationKindMap.size !== expected.size) {
    failures.push(`${language}: operationKind map count ${operationKindMap.size} != Go ${expected.size}`);
  }
  if (httpMethodMap.size !== expected.size) {
    failures.push(`${language}: HTTP method map count ${httpMethodMap.size} != Go ${expected.size}`);
  }
  if (httpPathMap.size !== expected.size) {
    failures.push(`${language}: HTTP path map count ${httpPathMap.size} != Go ${expected.size}`);
  }
}

function assertNoAcronymSplitPublicMethod(failures, language, text, pattern) {
  if (pattern.test(text)) {
    failures.push(`${language}: exposes raw acronym-split public method for SendOTP`);
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
  let generatedIdentities = 0;

  const templateChecks = [
    ["sdk-templates/typescript/generatedClient.ts.tmpl", "{{RPC_ALIAS_CAMEL}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/python/udb_client/generated_client.py.tmpl", "{{RPC_ALIAS_SNAKE}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/php/src/Generated/GeneratedClient.php.tmpl", "{{RPC_ALIAS_SNAKE}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl", "{{RPC_ALIAS_PASCAL}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/csharp/Udb.Client/GeneratedClient.cs.tmpl", "{{RPC_ALIAS_PASCAL}}", "{{RPC_WIRE_NAME}}"],
    ["sdk-templates/go/udbclient/generated_client.go.tmpl", "{{RPC_ALIAS_SNAKE}}", "{{REST_OPERATION_ID}}"],
  ];
  for (const [rel, aliasNeedle, wireNeedle] of templateChecks) {
    const text = readText(rel);
    if (!text.includes(aliasNeedle)) failures.push(`${rel}: missing ${aliasNeedle}`);
    if (!text.includes(wireNeedle)) failures.push(`${rel}: missing ${wireNeedle}`);
  }

  try {
    const goText = readText("sdk/go/udbclient/generated_client.go");
    const expected = parseGoIdentities(goText);
    generatedIdentities = expected.size;
    const tsText = readText("sdk/typescript/generatedClient.ts");
    const pyText = readText("sdk/python/udb_client/generated_client.py");
    const phpText = readText("sdk/php/src/Generated/GeneratedClient.php");
    const javaText = readText("sdk/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java");
    const csharpText = readText("sdk/csharp/Udb.Client/GeneratedClient.cs");

    compareSwaggerRoutes(failures, expected, swagger);
    compareFlatMaps(
      failures,
      "TypeScript",
      expected,
      parseQuotedMap(tsText, "RPC_API_ALIAS"),
      parseQuotedMap(tsText, "RPC_OPERATION_ID"),
      parseQuotedMap(tsText, "RPC_OPERATION_KIND"),
      parseQuotedMap(tsText, "RPC_HTTP_METHOD"),
      parseQuotedMap(tsText, "RPC_HTTP_PATH"),
    );
    compareFlatMaps(
      failures,
      "Python",
      expected,
      parseQuotedMap(pyText, "RPC_API_ALIAS"),
      parseQuotedMap(pyText, "RPC_OPERATION_ID"),
      parseQuotedMap(pyText, "RPC_OPERATION_KIND"),
      parseQuotedMap(pyText, "RPC_HTTP_METHOD"),
      parseQuotedMap(pyText, "RPC_HTTP_PATH"),
    );
    compareFlatMaps(
      failures,
      "PHP",
      expected,
      parseQuotedMap(phpText, "API_ALIAS"),
      parseQuotedMap(phpText, "OPERATION_ID"),
      parseQuotedMap(phpText, "OPERATION_KIND_BY_RPC"),
      parseQuotedMap(phpText, "HTTP_METHOD"),
      parseQuotedMap(phpText, "HTTP_PATH"),
      phpKeyFor,
    );
    compareMap(failures, "Java", expected, parseJavaIdentities(javaText));
    compareMap(failures, "C#", expected, parseCsharpIdentities(csharpText));

    const samples = [
      ["/udb.core.authn.services.v1.AuthnService/SendOTP", "send_otp", "sendOtp"],
      ["/udb.core.storage.services.v1.StorageService/DownloadFile", "download_file", "downloadFile"],
      ["/udb.services.v1.DataBroker/Select", "select", "select"],
    ];
    for (const [path, alias, operationId] of samples) {
      const info = expected.get(path);
      if (!info) {
        failures.push(`Go: missing sampled RPC ${path}`);
      } else if (info.apiAlias !== alias || info.operationId !== operationId) {
        failures.push(`Go: sampled RPC ${path} is ${info.apiAlias}/${info.operationId}, expected ${alias}/${operationId}`);
      }
    }
    assertNoAcronymSplitPublicMethod(failures, "TypeScript", tsText, /\bsend_o_t_p\s*\(/);
    assertNoAcronymSplitPublicMethod(failures, "Python", pyText, /\bdef\s+send_o_t_p\s*\(/);
    assertNoAcronymSplitPublicMethod(failures, "PHP", phpText, /public\s+function\s+send_o_t_p\s*\(/i);
    assertNoAcronymSplitPublicMethod(failures, "Java", javaText, /public\s+[^{;=]+?\s+SendOTP\s*\(/);
    assertNoAcronymSplitPublicMethod(failures, "C#", csharpText, /public\s+[^{;=]+?\s+SendOTPAsync\s*\(/);
  } catch (err) {
    failures.push(`generated SDK identity parse failed: ${err.message}`);
  }

  if (failures.length) {
    return { status: "FAIL", note: failures.slice(0, 8).join("; ") };
  }
  return { status: "PASS", note: `${operations} Swagger operations, ${aliasExtensions} SDK aliases, ${generatedIdentities} generated RPC identities aligned` };
}

const explicitTargets = process.argv.slice(2).filter((a) => TARGETS[a]);
const selected = explicitTargets;
const defaultTargets = Object.keys(LANGS);
const targets = selected.length ? selected : defaultTargets;
const strictSelected = selected.length > 0;

const results = [];
for (const name of targets) {
  const cfg = TARGETS[name];
  const ready = runnable(name, cfg);
  if (!ready.ok) {
    results.push({ name, status: ready.status, note: ready.note });
    continue;
  }
  process.stderr.write(`\n=== ${name} ===\n`);
  if (cfg.check) {
    results.push({ name, ...cfg.check() });
    continue;
  }
  if (cfg.setup) {
    const s = run(cfg.setup, cfg.cwd);
    if (s.code !== 0) {
      results.push({ name, status: "FAIL", note: "setup error" });
      process.stderr.write(s.out);
      continue;
    }
  }
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
