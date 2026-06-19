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
const nativeContractPath = resolve(repoRoot, 'docs/generated/udb-native-contract.json');

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

function toLowerCamel(value) {
  return String(value || '')
    .split(/[_\-\s]+/)
    .filter(Boolean)
    .map((part, index) => {
      const lower = part.toLowerCase();
      return index === 0 ? lower : lower.charAt(0).toUpperCase() + lower.slice(1);
    })
    .join('');
}

function normalizeSwaggerPath(path) {
  // The openapiv2 plugin may render proto field names as JSON-style path
  // variables. Compare route literals structurally and ignore variable spelling.
  return String(path || '').replace(/\{[^}]+\}/g, '{}');
}

function resourceFromPath(path) {
  const segments = String(path || '')
    .split('/')
    .filter(Boolean)
    .filter((segment) => segment !== 'v1');
  const resource = segments.find((segment) => !segment.startsWith('{'));
  return resource ? resource.replace(/:.+$/, '') : '';
}

function idempotencySummary(rpc) {
  const contract = rpc.idempotency_contract || {};
  return {
    request_key_field: contract.request_key_field || '',
    server_generated_key: Boolean(contract.server_generated_key),
    duplicate_response_field: contract.duplicate_response_field || '',
    replay_safe: Boolean(contract.replay_safe),
  };
}

function buildOperationMetadata() {
  let contract;
  try {
    contract = JSON.parse(readFileSync(nativeContractPath, 'utf8'));
  } catch (err) {
    console.warn(`openapi-postprocess: native contract unavailable, skipping descriptor operation metadata: ${err.message}`);
    return { byGeneratedId: new Map(), byRoute: new Map() };
  }

  const byGeneratedId = new Map();
  const byRoute = new Map();
  for (const service of contract.services || []) {
    const serviceName = String(service.service || '').split('.').pop();
    if (!serviceName) continue;
    for (const rpc of service.rpcs || []) {
      if (!rpc.http) continue;
      const sdkSurface = rpc.sdk_surface || {};
      const annotatedAlias = sdkSurface.method_alias || '';
      const restOperationId = sdkSurface.rest_operation_id || toLowerCamel(annotatedAlias) || toLowerCamel(rpc.method);
      const methodAlias = annotatedAlias || restOperationId;
      const operationKind = rpc.operation_kind || '';
      const metadata = {
        generatedId: `${serviceName}_${rpc.method}`,
        operationId: restOperationId,
        sdkAlias: methodAlias,
        scopes: Array.isArray(rpc.scopes) ? rpc.scopes : [],
        retrySafe: Boolean(rpc.idempotency_contract?.replay_safe || rpc.read_only),
        idempotency: idempotencySummary(rpc),
        resource: resourceFromPath(rpc.http.path),
        operationKind,
        routeKey: `${String(rpc.http.verb || '').toLowerCase()} ${normalizeSwaggerPath(rpc.http.path)}`,
      };
      byGeneratedId.set(metadata.generatedId, metadata);
      byRoute.set(metadata.routeKey, metadata);
    }
  }
  return { byGeneratedId, byRoute };
}

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
// Normalize actual file newlines too; otherwise the schema-shape fixups below
// only match fresh LF output from Linux CI and not a CRLF checkout.
text = text
  .replace(/\r\n/g, "\n")
  .replace(/\r/g, "\n")
  .replace(/\\r\\n/g, "\\n")
  .replace(/\\r/g, "\\n");

// OpenAPI plugin releases disagree on whether a leading source-comment heading
// belongs in a schema title or description. Canonicalize the handful of known
// shapes so cached local remote-plugin output and fresh CI output match.
function escaped(value) {
  return JSON.stringify(value).slice(1, -1);
}

const titledDescriptions = [
  '── Stage 2: native database fast-path access ──────────────────────────────',
  '── Stage 2: signed policy bundles for local SDK authorization caches ───────',
];

for (const title of titledDescriptions) {
  const titleEscaped = escaped(title);
  text = text.replace(
    new RegExp(`"description": "${titleEscaped}\\\\n\\\\n([^"]+)"`, 'g'),
    `"description": "$1"`,
  );
}

const schemaTitles = new Map([
  ['v1NativeAccessRequest', titledDescriptions[0]],
  ['v1PolicyBundleRequest', titledDescriptions[1]],
]);

const sameSchema = '(?:(?!\\n    "v1[A-Za-z0-9]+": \\{)[\\s\\S])*?';

for (const [schemaName, title] of schemaTitles) {
  text = text.replace(
    new RegExp(`("${schemaName}": \\{${sameSchema}\\n[ \\t]*\\},\\n[ \\t]*"description": "(?:[^"\\\\]|\\\\.)*")([,]?\\n[ \\t]*\\})`, 'm'),
    `$1,\n      "title": ${JSON.stringify(title)}$2`,
  );
}

for (const longCommentSchema of ['v1Session', 'v1User']) {
  text = text.replace(
    new RegExp(`("${longCommentSchema}": \\{${sameSchema}\\n[ \\t]*\\},\\n[ \\t]*)"title": ("(?:[^"\\\\]|\\\\.)*")`, 'm'),
    '$1"description": $2',
  );
}

if (!/"info":\s*\{[\s\S]*?"title":\s*"UDB Control-Plane API"[\s\S]*?"version":\s*"[^"]+"/m.test(text)) {
  console.error('openapi-postprocess: swagger info block shape changed?');
  process.exit(1);
}

const swagger = JSON.parse(text);
const operationMetadata = buildOperationMetadata();
let operationMetadataApplied = 0;
for (const [path, pathItem] of Object.entries(swagger.paths || {})) {
  for (const [verb, operation] of Object.entries(pathItem || {})) {
    if (!['get', 'put', 'post', 'patch', 'delete'].includes(verb) || !operation) {
      continue;
    }
    const routeKey = `${verb} ${normalizeSwaggerPath(path)}`;
    const metadata =
      operationMetadata.byGeneratedId.get(operation.operationId) ||
      operationMetadata.byRoute.get(routeKey);
    if (!metadata) {
      continue;
    }
    operation.operationId = metadata.operationId;
    operation['x-udb-sdk-alias'] = metadata.sdkAlias;
    operation['x-udb-scope'] = metadata.scopes;
    operation['x-udb-retry-safe'] = metadata.retrySafe;
    operation['x-udb-idempotency'] = metadata.idempotency;
    operation['x-udb-resource'] = metadata.resource;
    operation['x-udb-operation-kind'] = metadata.operationKind;
    operationMetadataApplied += 1;
  }
}

text = JSON.stringify(swagger, null, 2) + '\n';
writeFileSync(swaggerPath, text);
console.log(
  `openapi-postprocess: set title="${TITLE}", version="${udbVersion}", descriptor metadata on ${operationMetadataApplied} operations in api/udb-broker.swagger.json`,
);
