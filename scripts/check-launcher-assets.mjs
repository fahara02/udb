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

let failures = 0;
for (const file of LAUNCHERS) {
  if (!fs.existsSync(file)) {
    console.error(`MISSING launcher: ${file}`);
    failures++;
    continue;
  }
  const src = fs.readFileSync(file, "utf8");
  for (const [re, why] of FORBIDDEN) {
    const m = src.match(re);
    if (m) {
      console.error(`STALE ASSET SCHEME  ${file}: ${why}  (matched ${JSON.stringify(m[0])})`);
      failures++;
    }
  }
  for (const [re, why] of REQUIRED) {
    if (!re.test(src)) {
      console.error(`MISSING TOKEN       ${file}: expected ${why}`);
      failures++;
    }
  }
}

if (failures) {
  console.error(
    `\ncheck-launcher-assets: ${failures} problem(s). Every launcher must build ` +
      `udb-<os>-<arch>[-<variant>][.exe] — see scripts/check-launcher-assets.mjs and ` +
      `.github/workflows/release-binaries.yml.`,
  );
  process.exit(1);
}
console.log(`check-launcher-assets: all ${LAUNCHERS.length} launchers + templates conform to udb-<os>-<arch>[-<variant>][.exe].`);
