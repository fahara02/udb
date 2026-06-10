#!/usr/bin/env node
// scripts/openapi-postprocess.mjs — deterministic OpenAPI metadata fixup.
//
// `buf generate` (openapiv2 plugin, merge mode) titles the merged document
// after the first merged proto file (`udb/core/common/v1/db.proto`) and emits
// `"version": "version not set"`. It also only contains the services/RPCs that
// carry `google.api.http` annotations — the core DataBroker gRPC RPCs are
// gRPC-native and are intentionally absent. This script rewrites the `info`
// block so the published artifact is honestly titled and versioned:
//
//   title       → "UDB Control-Plane API"   (scoped rename — TODO_PASS2 #144/#145)
//   version     → udb component version from versions.json (#144)
//   description → notes the core DataBroker RPCs are gRPC-only (#145)
//
// It performs TARGETED text replacement (not a JSON re-serialize) so the rest
// of buf's output is preserved byte-for-byte; CI runs this immediately after
// `buf generate` before the drift diff, and the local gen scripts run it too,
// so the committed file always matches.

import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '..');
const swaggerPath = resolve(repoRoot, 'api/udb-broker.swagger.json');
const versionsPath = resolve(repoRoot, 'versions.json');

const TITLE = 'UDB Control-Plane API';
const DESCRIPTION =
  'HTTP/JSON (gRPC-gateway) surface for UDB control-plane services. ' +
  'The core DataBroker data-plane RPCs are gRPC-native and are not represented here.';

const versions = JSON.parse(readFileSync(versionsPath, 'utf8'));
const udbVersion = versions?.components?.udb?.version;
if (!udbVersion) {
  console.error('openapi-postprocess: could not read components.udb.version from versions.json');
  process.exit(1);
}

let text = readFileSync(swaggerPath, 'utf8');

// Replace the `info.title` value (first occurrence, inside the info block).
text = text.replace(
  /("info":\s*\{\s*"title":\s*)"(?:[^"\\]|\\.)*"/,
  `$1${JSON.stringify(TITLE)}`,
);
// Replace the `info.version` value that immediately follows the title.
text = text.replace(
  /("info":\s*\{\s*"title":\s*"(?:[^"\\]|\\.)*",\s*"version":\s*)"(?:[^"\\]|\\.)*"/,
  `$1${JSON.stringify(udbVersion)}`,
);
// Inject a `description` after the version (idempotent: skip if already present).
if (!text.includes(`"description": ${JSON.stringify(DESCRIPTION)}`)) {
  text = text.replace(
    /("info":\s*\{\s*"title":\s*"(?:[^"\\]|\\.)*",\s*"version":\s*"(?:[^"\\]|\\.)*")/,
    `$1,\n    "description": ${JSON.stringify(DESCRIPTION)}`,
  );
}

// The openapiv2 plugin preserves source comment newlines inside JSON string
// values. Normalize escaped CRLFs so Windows and Linux generation do not drift.
text = text.replace(/\\r\\n/g, "\\n").replace(/\\r/g, "\\n");

if (!/"info":\s*\{[\s\S]*?"title":\s*"UDB Control-Plane API"[\s\S]*?"version":\s*"[^"]+"/m.test(text)) {
  console.error('openapi-postprocess: swagger info block shape changed?');
  process.exit(1);
}

writeFileSync(swaggerPath, text);
console.log(`openapi-postprocess: set title="${TITLE}", version="${udbVersion}" in api/udb-broker.swagger.json`);
