#!/usr/bin/env node
// Conformance gate: every SDK binary launcher (and its regen template) MUST
// construct the canonical published release asset name
//     udb-<os>-<arch>[-<variant>][.exe]   (raw binary; os linux|darwin|windows,
//                                           arch amd64|arm64)
// and must NOT use any stale scheme (Rust target triple, version-in-filename,
// archive extension, or `macos` as an os token). This is the gate that was
// MISSING when all six launchers silently drifted into three incompatible
// broken schemes (2026-06-13) while CI stayed green. The published scheme is
// owned by .github/workflows/release-binaries.yml; keep this in lockstep.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const LAUNCHERS = [
  "sdk/go/cmd/udb/main.go",
  "sdk/python/udb_client/_cli.py",
  "sdk/typescript/bin/udb.js",
  "sdk/java/src/main/java/dev/udb/cli/Launcher.java",
  "sdk/csharp/Udb.Cli/UdbCli.cs",
  "sdk/php/bin/udb",
  // regen sources — must match the generated launchers above
  "sdk-templates/go/cmd/udb/main.go.tmpl",
  "sdk-templates/python/udb_client/_cli.py.tmpl",
  "sdk-templates/typescript/bin/udb.js.tmpl",
  "sdk-templates/java/src/main/java/dev/udb/cli/Launcher.java.tmpl",
  "sdk-templates/csharp/Udb.Cli/UdbCli.cs.tmpl",
  "sdk-templates/php/bin/udb.tmpl",
];

// Stale-scheme markers that must never appear in a launcher's asset construction.
const FORBIDDEN = [
  [/udb-v[\d{$"'`]/, "version embedded in the asset name (version belongs in the release tag, not the filename)"],
  [/unknown-linux-gnu|apple-darwin|pc-windows-msvc/, "Rust target triple in the asset name"],
  [/udb-[\w{}$.+-]*\.(?:tar\.gz|zip)\b/, "archive-named udb asset (the release ships RAW binaries)"],
  [/[:=>]\s*["']macos["']|["']macos["']\s*[,)\]]/, "`macos` os token (canonical is `darwin`)"],
];

// The canonical tokens that must be present (sanity that the launcher uses the scheme).
const REQUIRED = [
  [/\bdarwin\b/, "darwin os token"],
  [/\bwindows\b/, "windows os token"],
  [/\bamd64\b/, "amd64 arch token"],
  [/\barm64\b/, "arm64 arch token"],
  [/UDB_BIN_VARIANT/, "UDB_BIN_VARIANT tier support"],
];

function checkLaunchers(root = process.cwd()) {
  const failures = [];
  for (const file of LAUNCHERS) {
    const fullPath = path.join(root, file);
    if (!fs.existsSync(fullPath)) {
      failures.push(`MISSING launcher: ${file}`);
      continue;
    }
    const src = fs.readFileSync(fullPath, "utf8");
    for (const [re, why] of FORBIDDEN) {
      const m = src.match(re);
      if (m) {
        failures.push(`STALE ASSET SCHEME  ${file}: ${why}  (matched ${JSON.stringify(m[0])})`);
      }
    }
    for (const [re, why] of REQUIRED) {
      if (!re.test(src)) {
        failures.push(`MISSING TOKEN       ${file}: expected ${why}`);
      }
    }
  }
  return failures;
}

function writeFixtureLauncher(root, file, body) {
  const fullPath = path.join(root, file);
  fs.mkdirSync(path.dirname(fullPath), { recursive: true });
  fs.writeFileSync(fullPath, body, "utf8");
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function runSelftest() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "udb-launcher-assets-"));
  try {
    const goodLauncher = [
      "const osName = platform === 'darwin' ? 'darwin' : platform === 'windows' ? 'windows' : 'linux';",
      "const archName = arch === 'arm64' ? 'arm64' : 'amd64';",
      "const variant = process.env.UDB_BIN_VARIANT ? `-${process.env.UDB_BIN_VARIANT}` : '';",
      "const exe = osName === 'windows' ? '.exe' : '';",
      "const asset = `udb-${osName}-${archName}${variant}${exe}`;",
    ].join("\n");
    for (const file of LAUNCHERS) {
      writeFixtureLauncher(root, file, goodLauncher);
    }
    assert(checkLaunchers(root).length === 0, "good launcher fixture failed");

    writeFixtureLauncher(root, LAUNCHERS[0], goodLauncher.replaceAll("UDB_BIN_VARIANT", "UDB_TIER"));
    let failures = checkLaunchers(root);
    assert(
      failures.some((failure) => failure.includes("UDB_BIN_VARIANT tier support")),
      "selftest failed to reject missing UDB_BIN_VARIANT support",
    );
    writeFixtureLauncher(root, LAUNCHERS[0], goodLauncher);

    writeFixtureLauncher(root, LAUNCHERS[1], `${goodLauncher}\nconst stale = "x86_64-unknown-linux-gnu";\n`);
    failures = checkLaunchers(root);
    assert(
      failures.some((failure) => failure.includes("Rust target triple")),
      "selftest failed to reject Rust target triple asset scheme",
    );
    writeFixtureLauncher(root, LAUNCHERS[1], goodLauncher);

    fs.rmSync(path.join(root, LAUNCHERS[2]));
    failures = checkLaunchers(root);
    assert(
      failures.some((failure) => failure.includes(`MISSING launcher: ${LAUNCHERS[2]}`)),
      "selftest failed to reject missing launcher path",
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

function main(argv) {
  if (argv.includes("--selftest")) {
    runSelftest();
    console.log("check-launcher-assets selftest passed");
    return;
  }

  const failures = checkLaunchers();
  if (failures.length) {
    for (const failure of failures) {
      console.error(failure);
    }
    console.error(
      `\ncheck-launcher-assets: ${failures.length} problem(s). Every launcher must build ` +
        `udb-<os>-<arch>[-<variant>][.exe] — see scripts/check-launcher-assets.mjs and ` +
        `.github/workflows/release-binaries.yml.`,
    );
    process.exit(1);
  }
  console.log(
    `check-launcher-assets: all ${LAUNCHERS.length} launchers + templates conform to udb-<os>-<arch>[-<variant>][.exe].`,
  );
}

main(process.argv.slice(2));
