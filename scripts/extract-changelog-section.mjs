// Extract one version's section from CHANGELOG.md for the release body (W1,
// 0.4.23). Usage: node scripts/extract-changelog-section.mjs <version> [--out <file>]
// Prints the section between `## [<version>]` (or `## <version>`) and the next
// `## ` heading; exits 1 when the section is missing or empty so the release
// pipeline FAILS CLOSED instead of publishing an unexplained release.
import { readFileSync, writeFileSync } from "node:fs";

const version = process.argv[2];
if (!version) {
  console.error("usage: node scripts/extract-changelog-section.mjs <version> [--out <file>]");
  process.exit(2);
}
const outIdx = process.argv.indexOf("--out");
const outPath = outIdx > -1 ? process.argv[outIdx + 1] : null;

const text = readFileSync("CHANGELOG.md", "utf8").replaceAll("\r\n", "\n");
const pattern = new RegExp(
  `^## \\[?${version.replace(/[.[\]]/g, (ch) => `\\${ch}`)}\\]?[^\n]*$`,
  "m",
);
const match = pattern.exec(text);
if (!match) {
  console.error(`CHANGELOG.md has no section for version ${version} — write the consumer-facing notes before releasing`);
  process.exit(1);
}
const start = match.index + match[0].length;
const rest = text.slice(start);
const next = rest.search(/^## /m);
const body = (next === -1 ? rest : rest.slice(0, next)).trim();
if (!body) {
  console.error(`CHANGELOG.md section for ${version} is empty — write the consumer-facing notes before releasing`);
  process.exit(1);
}
if (outPath) {
  writeFileSync(outPath, body + "\n", "utf8");
  console.error(`wrote ${outPath} (${body.length} bytes)`);
} else {
  process.stdout.write(body + "\n");
}
