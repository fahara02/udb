#!/usr/bin/env node
// Generate manifest.json for a UDB binary release: the single contract that
// lists every published asset with its os/arch/tier + sha256 + size, parsed
// from the canonical name `udb-<os>-<arch>[-<tier>][.exe]`. Launchers (and the
// conformance gate) can resolve assets from this instead of reconstructing
// names, which is what let all six launchers drift. Attached to the GitHub
// release alongside the binaries by .github/workflows/release-binaries.yml.
//
// Usage: node scripts/gen-release-manifest.mjs <dist-dir> [version] > manifest.json
import fs from "node:fs";
import path from "node:path";

const dir = process.argv[2] || "dist";
const version =
  process.argv[3] ||
  JSON.parse(fs.readFileSync("versions.json", "utf8")).components.udb.version;

const NAME_RE = /^udb-(linux|darwin|windows)-(amd64|arm64)(?:-([a-z0-9]+))?(\.exe)?$/;

const assets = [];
for (const name of fs.readdirSync(dir).sort()) {
  if (!name.startsWith("udb-") || name.endsWith(".sha256") || name === "manifest.json") {
    continue;
  }
  const m = name.match(NAME_RE);
  if (!m) {
    console.error(`gen-release-manifest: WARNING skipping unrecognized asset name: ${name}`);
    continue;
  }
  const [, os, arch, tier, exe] = m;
  let sha256 = null;
  const shaPath = path.join(dir, `${name}.sha256`);
  if (fs.existsSync(shaPath)) {
    sha256 = fs.readFileSync(shaPath, "utf8").trim().split(/\s+/)[0] || null;
  } else {
    console.error(`gen-release-manifest: WARNING no .sha256 sidecar for ${name}`);
  }
  assets.push({
    name,
    os,
    arch,
    tier: tier || "portable",
    ext: exe || "",
    sha256,
    size: fs.statSync(path.join(dir, name)).size,
  });
}

if (assets.length === 0) {
  console.error(`gen-release-manifest: no udb-* assets found in ${dir}`);
  process.exit(1);
}

const manifest = {
  version,
  tag: `v${version}`,
  scheme: "udb-<os>-<arch>[-<tier>][.exe]",
  base_url: `https://github.com/fahara02/udb/releases/download/v${version}`,
  assets,
};
process.stdout.write(JSON.stringify(manifest, null, 2) + "\n");
