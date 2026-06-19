#!/usr/bin/env node
// Validate the generated public OpenAPI document against docs/api-rules.md.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '..');
const swaggerPath = resolve(repoRoot, 'api/udb-broker.swagger.json');

const swagger = JSON.parse(readFileSync(swaggerPath, 'utf8'));
const errors = [];

function add(path, message) {
  errors.push(`${path}: ${message}`);
}

function normalizedOperationId(id) {
  return String(id || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

function pathLiteralSegments(path) {
  return String(path || '')
    .split('/')
    .filter(Boolean)
    .filter((segment) => !segment.startsWith('{'));
}

function isAllowedLiteral(segment, path) {
  if (segment === 'v1') return true;
  if (segment === '.well-known' || segment === 'jwks.json') return true;
  if ((segment === 'Users' || segment === 'Groups') && path.includes('/scim/')) return true;
  return false;
}

function isKebabLiteral(segment) {
  const base = segment.replace(/:.+$/, '');
  return /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(base);
}

function actionSuffix(segment) {
  const index = segment.indexOf(':');
  return index === -1 ? '' : segment.slice(index + 1);
}

function isLowerCamel(value) {
  return /^[a-z][A-Za-z0-9]*$/.test(value) && !value.includes('_') && !value.includes('-');
}

const exactIds = new Map();
const normalizedIds = new Map();
const snakeIds = new Map();
const camelIds = new Map();
const pascalIds = new Map();

const retiredBetaRoutes = new Set([
  '/v1/auth/authenticate',
  '/v1/auth/login',
  '/v1/auth/logout',
  '/v1/authz/governance/canaries:status',
  '/v1/authz/governance/explain',
  '/v1/authz/governance/revision',
  '/v1/authz/governance/simulate',
  '/v1/storage/uploads/{fileId}/finalize',
  '/v1/storage/uploads/{file_id}/finalize',
  '/v1/webrtc/rooms/{roomId}/close',
  '/v1/webrtc/rooms/{room_id}/close',
  '/v1/webrtc/tracks/{trackId}/mute',
  '/v1/webrtc/tracks/{track_id}/mute',
]);

function words(value) {
  return String(value || '')
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .split(/[^A-Za-z0-9]+/)
    .filter(Boolean)
    .map((word) => word.toLowerCase());
}

function lowerCamel(value) {
  const parts = words(value);
  return parts
    .map((word, index) => index === 0 ? word : word[0].toUpperCase() + word.slice(1))
    .join('');
}

function pascal(value) {
  return words(value)
    .map((word) => word[0].toUpperCase() + word.slice(1))
    .join('');
}

function snake(value) {
  return words(value).join('_');
}

function assertUnique(map, key, id, where, mode) {
  const previous = map.get(key);
  if (previous && previous.id !== id) {
    add(where, `operationId "${id}" collides with "${previous.id}" after ${mode} normalization at ${previous.where}`);
  } else {
    map.set(key, { id, where });
  }
}

for (const [path, pathItem] of Object.entries(swagger.paths || {})) {
  if (retiredBetaRoutes.has(path)) {
    add(path, 'retired beta route shape must not reappear; use the resource-oriented v1 route');
  }
  if (!path.startsWith('/v1/') && !path.startsWith('/.well-known/')) {
    add(path, 'public OpenAPI path must start with /v1 or an allowed well-known prefix');
  }
  if (path.endsWith('/') && path !== '/') {
    add(path, 'trailing slash is not allowed');
  }

  for (const segment of pathLiteralSegments(path)) {
    if (isAllowedLiteral(segment, path)) continue;
    if (segment.includes('_')) {
      add(path, `literal path segment "${segment}" uses snake_case`);
    }
    if (!isKebabLiteral(segment)) {
      add(path, `literal path segment "${segment}" must be lowercase kebab-case`);
    }
    const action = actionSuffix(segment);
    if (action && !isLowerCamel(action)) {
      add(path, `custom action "${action}" must be lowerCamelCase`);
    }
  }

  for (const [verb, operation] of Object.entries(pathItem || {})) {
    if (!['get', 'put', 'post', 'patch', 'delete'].includes(verb)) continue;
    const where = `${verb.toUpperCase()} ${path}`;
    const operationId = operation?.operationId || '';
    if (!operationId) {
      add(where, 'operationId is required');
    }
    if (/^[A-Za-z0-9]+Service_[A-Za-z0-9]+$/.test(operationId)) {
      add(where, `operationId "${operationId}" still has generated Service_RpcName shape`);
    }
    if (!isLowerCamel(operationId)) {
      add(where, `operationId "${operationId}" must be lowerCamelCase`);
    }

    const previousExact = exactIds.get(operationId);
    if (previousExact) {
      add(where, `operationId "${operationId}" duplicates ${previousExact}`);
    } else {
      exactIds.set(operationId, where);
    }

    const normalized = normalizedOperationId(operationId);
    const previousNormalized = normalizedIds.get(normalized);
    if (previousNormalized && previousNormalized.id !== operationId) {
      add(where, `operationId "${operationId}" collides with "${previousNormalized.id}" after SDK normalization at ${previousNormalized.where}`);
    } else {
      normalizedIds.set(normalized, { id: operationId, where });
    }
    assertUnique(snakeIds, snake(operationId), operationId, where, 'snake_case');
    assertUnique(camelIds, lowerCamel(operationId), operationId, where, 'lowerCamel');
    assertUnique(pascalIds, pascal(operationId), operationId, where, 'PascalCase');

    for (const extension of [
      'x-udb-sdk-alias',
      'x-udb-scope',
      'x-udb-retry-safe',
      'x-udb-idempotency',
      'x-udb-resource',
      'x-udb-operation-kind',
    ]) {
      if (!(extension in operation)) {
        add(where, `missing descriptor-owned ${extension}`);
      }
    }

    for (const parameter of operation.parameters || []) {
      if (parameter.in === 'query' && ['action', 'op', 'operation'].includes(parameter.name)) {
        add(where, `query parameter "${parameter.name}" must not dispatch commands`);
      }
    }
  }
}

if (errors.length > 0) {
  console.error(`OpenAPI API-rule check failed with ${errors.length} violation(s):`);
  for (const error of errors) {
    console.error(`- ${error}`);
  }
  process.exit(1);
}

console.log(`OpenAPI API-rule check passed for ${exactIds.size} operation(s).`);
