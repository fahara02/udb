#!/usr/bin/env node
// Validate the generated public OpenAPI document against docs/api-rules.md.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '..');
const swaggerPath = resolve(repoRoot, 'api/udb-broker.swagger.json');

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

const descriptorOwnedExtensions = [
  'x-udb-sdk-alias',
  'x-udb-scope',
  'x-udb-retry-safe',
  'x-udb-idempotency',
  'x-udb-resource',
  'x-udb-operation-kind',
];
const apiErrorRef = '#/definitions/v1ApiError';
const jsonMediaType = 'application/json';
const requiredApiErrorFields = ['code', 'message', 'httpStatusCode', 'retryable', 'fieldViolations'];
const grpcHttpStatusMap = {
  400: ['INVALID_ARGUMENT', 'FAILED_PRECONDITION'],
  401: ['UNAUTHENTICATED'],
  403: ['PERMISSION_DENIED'],
  404: ['NOT_FOUND'],
  409: ['ALREADY_EXISTS', 'ABORTED'],
  429: ['RESOURCE_EXHAUSTED'],
  500: ['UNKNOWN', 'INTERNAL', 'DATA_LOSS'],
  501: ['UNIMPLEMENTED'],
  503: ['UNAVAILABLE'],
  504: ['DEADLINE_EXCEEDED'],
};
const forbiddenSuccessWrapperRefs = new Set([
  '#/definitions/v1ApiResponse',
  '#/definitions/v1RawJsonResponse',
]);

const betaStabilityClaim = /\b(?:stable\s+(?:backward\s+)?compatib(?:le|ility)|backward[-\s]+compatib(?:le|ility)|generally\s+available|\bGA\b|permanent\s+(?:route|SDK|support))/i;

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
    .map((word, index) => (index === 0 ? word : word[0].toUpperCase() + word.slice(1)))
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

function assertUnique(errors, map, key, id, where, mode) {
  const previous = map.get(key);
  if (previous && previous.id !== id) {
    errors.push(`${where}: operationId "${id}" collides with "${previous.id}" after ${mode} normalization at ${previous.where}`);
  } else {
    map.set(key, { id, where });
  }
}

function isThirdPartyCompatibilityText(value) {
  const text = String(value || '');
  return text.includes('google.protobuf.Type') || text.includes('binary compatibility needs to be preserved');
}

function scanBetaStabilityClaim(errors, where, value) {
  const text = String(value || '');
  if (!text || isThirdPartyCompatibilityText(text)) return;
  const hit = text.match(betaStabilityClaim);
  if (hit) {
    errors.push(`${where}: description must not promise pre-1.0 API/SDK stability (${hit[0]})`);
  }
}

function scanSchemaDescriptions(errors, value, where = 'schema') {
  if (!value || typeof value !== 'object') return;
  if (typeof value.description === 'string') {
    scanBetaStabilityClaim(errors, `${where}.description`, value.description);
  }
  for (const [key, child] of Object.entries(value)) {
    if (key === 'description') continue;
    if (child && typeof child === 'object') {
      scanSchemaDescriptions(errors, child, `${where}.${key}`);
    }
  }
}

function schemaRef(schema) {
  return schema && typeof schema === 'object' ? String(schema.$ref || '') : '';
}

function isApiErrorSchema(schema) {
  return schemaRef(schema) === apiErrorRef;
}

function isForbiddenSuccessWrapper(schema) {
  if (!schema || typeof schema !== 'object') return false;
  const ref = schemaRef(schema);
  if (forbiddenSuccessWrapperRefs.has(ref)) return true;
  const properties = schema.properties || {};
  return Boolean(properties.success && (properties.data || properties.error));
}

function hasAllGrpcCodes(response, requiredCodes) {
  const actual = Array.isArray(response?.['x-udb-grpc-codes']) ? response['x-udb-grpc-codes'] : [];
  return requiredCodes.every((code) => actual.includes(code));
}

function mediaTypes(value) {
  return Array.isArray(value) ? value.map((entry) => String(entry || '').toLowerCase()) : [];
}

function validateRestMediaBoundary(errors, swagger) {
  const consumes = mediaTypes(swagger.consumes);
  const produces = mediaTypes(swagger.produces);
  if (!consumes.includes(jsonMediaType)) {
    errors.push(`root.consumes must include ${jsonMediaType} for REST JSON requests`);
  }
  if (!produces.includes(jsonMediaType)) {
    errors.push(`root.produces must include ${jsonMediaType} for REST JSON success/error bodies`);
  }
}

function validateApiErrorDefinition(errors, swagger) {
  const apiError = swagger.definitions?.v1ApiError;
  if (!apiError) {
    errors.push('definitions.v1ApiError: REST error boundary must expose ApiError, not grpc-gateway rpcStatus');
    return;
  }
  const properties = apiError.properties || {};
  for (const field of requiredApiErrorFields) {
    if (!(field in properties)) {
      errors.push(`definitions.v1ApiError: missing public error field "${field}"`);
    }
  }
}

function checkSwagger(swagger) {
  const errors = [];
  const exactIds = new Map();
  const normalizedIds = new Map();
  const snakeIds = new Map();
  const camelIds = new Map();
  const pascalIds = new Map();

  function add(path, message) {
    errors.push(`${path}: ${message}`);
  }

  validateRestMediaBoundary(errors, swagger);
  validateApiErrorDefinition(errors, swagger);

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
      scanBetaStabilityClaim(errors, `${where} summary`, operation?.summary);
      scanBetaStabilityClaim(errors, `${where} description`, operation?.description);

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
      assertUnique(errors, snakeIds, snake(operationId), operationId, where, 'snake_case');
      assertUnique(errors, camelIds, lowerCamel(operationId), operationId, where, 'lowerCamel');
      assertUnique(errors, pascalIds, pascal(operationId), operationId, where, 'PascalCase');

      for (const extension of descriptorOwnedExtensions) {
        if (!(extension in operation)) {
          add(where, `missing descriptor-owned ${extension}`);
        }
      }

      for (const parameter of operation.parameters || []) {
        if (parameter.in === 'query' && ['action', 'op', 'operation'].includes(parameter.name)) {
          add(where, `query parameter "${parameter.name}" must not dispatch commands`);
        }
      }

      const responses = operation.responses || {};
      const successResponses = Object.entries(responses).filter(([status]) => /^2\d\d$/.test(status));
      if (successResponses.length === 0) {
        add(where, 'must define a bare 2xx success response');
      }
      for (const [status, response] of successResponses) {
        if (isForbiddenSuccessWrapper(response?.schema)) {
          add(where, `${status} success response must be a bare typed body, not ApiResponse/RawJsonResponse or a {success,data,error} envelope`);
        }
      }

      for (const [status, grpcCodes] of Object.entries(grpcHttpStatusMap)) {
        const response = responses[status];
        if (!response) {
          add(where, `missing REST error response ${status} for gRPC ${grpcCodes.join('/')}`);
          continue;
        }
        if (!isApiErrorSchema(response.schema)) {
          add(where, `${status} REST error response must use ${apiErrorRef}, not ${schemaRef(response.schema) || 'an inline/empty schema'}`);
        }
        if (!hasAllGrpcCodes(response, grpcCodes)) {
          add(where, `${status} REST error response must preserve canonical gRPC code(s) ${grpcCodes.join(', ')} in x-udb-grpc-codes`);
        }
      }
      if (!responses.default || !isApiErrorSchema(responses.default.schema)) {
        add(where, `default REST error response must use ${apiErrorRef}`);
      }
    }
  }

  for (const [name, schema] of Object.entries(swagger.definitions || {})) {
    scanSchemaDescriptions(errors, schema, `definitions.${name}`);
  }
  for (const [name, schema] of Object.entries(swagger.components?.schemas || {})) {
    scanSchemaDescriptions(errors, schema, `components.schemas.${name}`);
  }

  return { errors, operationCount: exactIds.size };
}

function apiErrorDefinition() {
  return {
    type: 'object',
    properties: {
      code: { type: 'string' },
      message: { type: 'string' },
      httpStatusCode: { type: 'integer' },
      retryable: { type: 'boolean' },
      fieldViolations: { type: 'array', items: { type: 'object' } },
    },
  };
}

function errorResponse(status, grpcCodes) {
  return {
    description: `gRPC ${grpcCodes.join('/')} mapped to HTTP ${status}.`,
    schema: { $ref: apiErrorRef },
    'x-udb-grpc-codes': grpcCodes,
  };
}

function restBoundaryResponses(successRef = '#/definitions/v1Thing') {
  const responses = {
    200: {
      description: 'A successful response.',
      schema: { $ref: successRef },
    },
    default: errorResponse('default', ['UNKNOWN']),
  };
  for (const [status, grpcCodes] of Object.entries(grpcHttpStatusMap)) {
    responses[status] = errorResponse(status, grpcCodes);
  }
  return responses;
}

function swaggerFixture(paths, overrides = {}) {
  return {
    consumes: [jsonMediaType],
    produces: [jsonMediaType],
    definitions: {
      v1ApiError: apiErrorDefinition(),
      v1Thing: { type: 'object', properties: { id: { type: 'string' } } },
      ...(overrides.definitions || {}),
    },
    paths,
    ...Object.fromEntries(Object.entries(overrides).filter(([key]) => key !== 'definitions')),
  };
}

function operation(id, overrides = {}) {
  return {
    operationId: id,
    responses: restBoundaryResponses(),
    'x-udb-sdk-alias': id,
    'x-udb-scope': 'udb:test',
    'x-udb-retry-safe': true,
    'x-udb-idempotency': 'READ_ONLY',
    'x-udb-resource': 'test',
    'x-udb-operation-kind': 'read_only',
    ...overrides,
  };
}

function assertSelftest(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function runSelftest() {
  let result = checkSwagger(swaggerFixture({
      '/v1/tenants/{tenantId}': {
        get: operation('getTenant'),
      },
      '/.well-known/jwks.json': {
        get: operation('getJwks'),
      },
  }));
  assertSelftest(result.errors.length === 0, `good OpenAPI fixture failed:\n${result.errors.join('\n')}`);

  result = checkSwagger(swaggerFixture({ '/v1/auth/login': { post: operation('login') } }));
  assertSelftest(
    result.errors.some((error) => error.includes('retired beta route shape')),
    `retired route regression was not caught:\n${result.errors.join('\n')}`,
  );

  result = checkSwagger(swaggerFixture({ '/v1/bad_path/{id}': { get: operation('BadService_Get') } }));
  assertSelftest(
    result.errors.some((error) => error.includes('uses snake_case')) &&
      result.errors.some((error) => error.includes('Service_RpcName shape')) &&
      result.errors.some((error) => error.includes('must be lowerCamelCase')),
    `path/operation naming regressions were not caught:\n${result.errors.join('\n')}`,
  );

  result = checkSwagger(swaggerFixture({ '/v1/things:DoThing': { post: operation('doThing') } }));
  assertSelftest(
    result.errors.some((error) => error.includes('custom action "DoThing" must be lowerCamelCase')),
    `custom action case regression was not caught:\n${result.errors.join('\n')}`,
  );

  const missingExtension = operation('createThing');
  delete missingExtension['x-udb-scope'];
  result = checkSwagger(swaggerFixture({ '/v1/things': { post: missingExtension } }));
  assertSelftest(
    result.errors.some((error) => error.includes('missing descriptor-owned x-udb-scope')),
    `missing descriptor extension was not caught:\n${result.errors.join('\n')}`,
  );

  result = checkSwagger(swaggerFixture({
      '/v1/alpha': { get: operation('getTenant') },
      '/v1/beta': { get: operation('get-tenant') },
  }));
  assertSelftest(
    result.errors.some((error) => error.includes('after SDK normalization')),
    `SDK-normalized operationId collision was not caught:\n${result.errors.join('\n')}`,
  );

  result = checkSwagger(swaggerFixture({
      '/v1/search': {
        get: operation('searchThings', { parameters: [{ in: 'query', name: 'action' }] }),
      },
  }));
  assertSelftest(
    result.errors.some((error) => error.includes('must not dispatch commands')),
    `query dispatch parameter was not caught:\n${result.errors.join('\n')}`,
  );

  result = checkSwagger(swaggerFixture({
      '/v1/stability': {
        get: operation('getStability', { description: 'This endpoint is GA with stable backward compatibility.' }),
      },
  }));
  assertSelftest(
    result.errors.some((error) => error.includes('must not promise pre-1.0 API/SDK stability')),
    `beta stability wording was not caught:\n${result.errors.join('\n')}`,
  );

  result = checkSwagger(swaggerFixture({
    '/v1/things/{thingId}': {
      get: operation('getThing', {
        responses: {
          ...restBoundaryResponses(),
          default: {
            description: 'An unexpected error response.',
            schema: { $ref: '#/definitions/rpcStatus' },
          },
        },
      }),
    },
  }));
  assertSelftest(
    result.errors.some((error) => error.includes('default REST error response must use #/definitions/v1ApiError')),
    `stale rpcStatus default response was not caught:\n${result.errors.join('\n')}`,
  );

  const missingNotFound = restBoundaryResponses();
  delete missingNotFound[404];
  result = checkSwagger(swaggerFixture({
    '/v1/things/{thingId}': {
      get: operation('getThing', { responses: missingNotFound }),
    },
  }));
  assertSelftest(
    result.errors.some((error) => error.includes('missing REST error response 404 for gRPC NOT_FOUND')),
    `missing NOT_FOUND->404 response was not caught:\n${result.errors.join('\n')}`,
  );

  result = checkSwagger(swaggerFixture({
    '/v1/wrapped': {
      get: operation('getWrapped', {
        responses: {
          ...restBoundaryResponses(),
          200: {
            description: 'A wrapped response.',
            schema: { $ref: '#/definitions/v1ApiResponse' },
          },
        },
      }),
    },
  }));
  assertSelftest(
    result.errors.some((error) => error.includes('success response must be a bare typed body')),
    `success envelope response was not caught:\n${result.errors.join('\n')}`,
  );

  result = checkSwagger(swaggerFixture({
    '/v1/plain': {
      get: operation('getPlain'),
    },
  }, {
    produces: ['text/plain'],
  }));
  assertSelftest(
    result.errors.some((error) => error.includes('root.produces must include application/json')),
    `REST content-type regression was not caught:\n${result.errors.join('\n')}`,
  );

  console.log('OpenAPI API-rule selftest passed');
}

if (process.argv.includes('--selftest')) {
  runSelftest();
  process.exit(0);
}

const swagger = JSON.parse(readFileSync(swaggerPath, 'utf8'));
const { errors, operationCount } = checkSwagger(swagger);

if (errors.length > 0) {
  console.error(`OpenAPI API-rule check failed with ${errors.length} violation(s):`);
  for (const error of errors) {
    console.error(`- ${error}`);
  }
  process.exit(1);
}

console.log(`OpenAPI API-rule check passed for ${operationCount} operation(s).`);
