#!/usr/bin/env node
// Inventory and validate descriptor-owned public HTTP route style.

import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '..');
const nativeContractPath = resolve(repoRoot, 'docs/generated/udb-native-contract.json');
const allowPath = resolve(repoRoot, 'scripts/http-api-style.allow.json');

const httpMethods = new Set(['get', 'put', 'post', 'patch', 'delete']);
const pseudoReadActions = new Set(['get', 'list', 'status']);
const slashVerbSegments = new Set([
  'activate',
  'approve',
  'authenticate',
  'change',
  'close',
  'complete',
  'deactivate',
  'emergency-revoke',
  'finalize',
  'login',
  'logout',
  'mute',
  'refresh',
  'reject',
  'resend',
  'reset',
  'rotate',
  'send',
  'start',
  'stop',
  'unpublish',
  'validate',
  'verify',
]);
const slashReadActionSegments = new Set([
  'download',
  'download-url',
]);
const singularCollectionSegments = new Set([
  'api-key',
  'asset',
  'credential',
  'file',
  'notification',
  'pipeline',
  'room',
  'tenant',
  'track',
  'upload',
  'user',
]);

function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

function listFiles(dir, suffix) {
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = resolve(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...listFiles(path, suffix));
    } else if (entry.isFile() && path.endsWith(suffix)) {
      out.push(path);
    }
  }
  return out;
}

function findMatchingBrace(text, openAt) {
  let depth = 0;
  for (let i = openAt; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '{') depth += 1;
    if (ch === '}') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
}

function sourceIndex(root) {
  const protoRoot = resolve(root, 'proto/udb/core');
  const index = new Map();
  let files = [];
  try {
    files = listFiles(protoRoot, '.proto');
  } catch {
    return index;
  }

  for (const path of files) {
    const text = readFileSync(path, 'utf8');
    const pkg = text.match(/\bpackage\s+([A-Za-z0-9_.]+)\s*;/)?.[1] || '';
    for (const serviceMatch of text.matchAll(/\bservice\s+([A-Za-z0-9_]+)\s*\{/g)) {
      const service = serviceMatch[1];
      const openAt = serviceMatch.index + serviceMatch[0].lastIndexOf('{');
      const closeAt = findMatchingBrace(text, openAt);
      if (closeAt < 0) continue;
      const body = text.slice(openAt, closeAt + 1);
      for (const rpcMatch of body.matchAll(/\brpc\s+([A-Za-z0-9_]+)\s*\(/g)) {
        const key = `${pkg}.${service}.${rpcMatch[1]}`;
        index.set(key, relative(root, path).replace(/\\/g, '/'));
      }
    }
  }
  return index;
}

function toRegex(entry) {
  return new RegExp(entry.pathPattern);
}

function compileAllowlist(allow) {
  return {
    allowedLiteralSegments: (allow.allowedLiteralSegments || []).map((entry) => ({ ...entry, regex: toRegex(entry) })),
    allowedDeepPaths: (allow.allowedDeepPaths || []).map((entry) => ({ ...entry, regex: toRegex(entry) })),
    allowedCommandEndpoints: (allow.allowedCommandEndpoints || []).map((entry) => ({ ...entry, regex: toRegex(entry) })),
  };
}

function matchingReason(entries, path) {
  const hit = entries.find((entry) => entry.regex.test(path));
  return hit?.reason || '';
}

function splitPath(path) {
  return String(path || '').split('/').filter(Boolean);
}

function literalSegments(path) {
  return splitPath(path).filter((segment) => !segment.startsWith('{'));
}

function splitAction(segment) {
  const at = segment.indexOf(':');
  if (at < 0) return { base: segment, action: '' };
  return { base: segment.slice(0, at), action: segment.slice(at + 1) };
}

function isKebab(value) {
  return /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(value);
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
  return words(value)
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

function isAllowedLiteral(segment, path, allow) {
  if (segment === 'v1') return true;
  return Boolean(matchingReason(allow.allowedLiteralSegments, path));
}

function isProbablyCommandEndpoint(verb, path, rpc) {
  if (String(verb).toLowerCase() !== 'post') return false;
  if (String(path || '').includes(':')) return false;
  const lastSegment = splitPath(path).at(-1) || '';
  const { base } = splitAction(lastSegment);
  return slashVerbSegments.has(base) && /^(Activate|Approve|Authenticate|Change|Close|Complete|Deactivate|Emergency|Finalize|Login|Logout|Mute|Refresh|Reject|Resend|Reset|Rotate|Send|Stop|Unpublish|Validate|Verify)/.test(rpc);
}

function routeFlags(route, allow) {
  const flags = [];
  const { verb, path, rpc } = route;
  if (!path.startsWith('/v1/') && !path.startsWith('/.well-known/')) {
    flags.push({ code: 'bad_prefix', message: 'public HTTP route must start with /v1 or an allowlisted well-known prefix' });
  }
  if (path.endsWith('/') && path !== '/') {
    flags.push({ code: 'trailing_slash', message: 'trailing slash is not allowed' });
  }

  for (const segment of literalSegments(path)) {
    if (isAllowedLiteral(segment, path, allow)) continue;
    const { base, action } = splitAction(segment);
    if (segment.includes('_')) {
      flags.push({ code: 'snake_case_literal', message: `literal segment "${segment}" uses snake_case` });
    }
    if (base && !isKebab(base)) {
      flags.push({ code: 'non_kebab_literal', message: `literal segment "${base}" must be lowercase kebab-case` });
    }
    if (singularCollectionSegments.has(base)) {
      flags.push({ code: 'singular_collection', message: `literal segment "${base}" looks like a singular collection name` });
    }
    if (action && !isLowerCamel(action)) {
      flags.push({ code: 'bad_action_case', message: `custom action "${action}" must be lowerCamelCase` });
    }
    if (pseudoReadActions.has(action)) {
      flags.push({ code: 'pseudo_read_action', message: `pseudo-read custom action ":${action}" should be modeled as a resource read/list` });
    }
    if (!action && slashVerbSegments.has(base)) {
      flags.push({ code: 'slash_verb', message: `slash verb segment "${base}" should be a colon action` });
    }
    if (!action && slashReadActionSegments.has(base)) {
      flags.push({ code: 'slash_read_action', message: `slash read action segment "${base}" should be a colon action or resource` });
    }
  }

  if (splitPath(path).length > 6 && !matchingReason(allow.allowedDeepPaths, path)) {
    flags.push({ code: 'deep_path_review', message: 'path depth exceeds collection/item/collection; add an allowlist reason or simplify' });
  }
  if (isProbablyCommandEndpoint(verb, path, rpc) && !matchingReason(allow.allowedCommandEndpoints, path)) {
    flags.push({ code: 'non_resource_command', message: `RPC ${rpc} looks command-like but route has no colon action` });
  }
  return flags;
}

function inventory(root = repoRoot) {
  const contract = readJson(resolve(root, 'docs/generated/udb-native-contract.json'));
  const sources = sourceIndex(root);
  const rows = [];
  for (const service of contract.services || []) {
    for (const rpc of service.rpcs || []) {
      if (!rpc.http) continue;
      const verb = String(rpc.http.verb || '').toLowerCase();
      if (!httpMethods.has(verb)) continue;
      const key = `${service.service}.${rpc.method}`;
      rows.push({
        service: service.service,
        rpc: rpc.method,
        method: verb.toUpperCase(),
        path: rpc.http.path,
        source: sources.get(key) || '',
        operationId: rpc.sdk_surface?.rest_operation_id || '',
        reasonFlags: [],
      });
    }
  }
  return rows;
}

function protoHttpInventory(root = repoRoot) {
  const protoRoot = resolve(root, 'proto/udb/core');
  const rows = [];
  let files = [];
  try {
    files = listFiles(protoRoot, '.proto');
  } catch {
    return rows;
  }

  for (const path of files) {
    const text = readFileSync(path, 'utf8');
    const pkg = text.match(/\bpackage\s+([A-Za-z0-9_.]+)\s*;/)?.[1] || '';
    for (const serviceMatch of text.matchAll(/\bservice\s+([A-Za-z0-9_]+)\s*\{/g)) {
      const service = serviceMatch[1];
      const openAt = serviceMatch.index + serviceMatch[0].lastIndexOf('{');
      const closeAt = findMatchingBrace(text, openAt);
      if (closeAt < 0) continue;
      const body = text.slice(openAt, closeAt + 1);
      for (const rpcMatch of body.matchAll(/^  rpc\s+([A-Za-z0-9_]+)\([\s\S]*?^  \}/gm)) {
        const rpc = rpcMatch[1];
        const block = rpcMatch[0];
        const route = block.match(/\b(get|post|put|patch|delete)\s*:\s*"([^"]+)"/);
        if (!route) continue;
        const verb = route[1].toLowerCase();
        rows.push({
          service: `${pkg}.${service}`,
          rpc,
          method: verb.toUpperCase(),
          path: route[2],
          source: relative(root, path).replace(/\\/g, '/'),
          operationId: block.match(/\brest_operation_id\s*:\s*"([^"]+)"/)?.[1] || '',
          reasonFlags: [],
        });
      }
    }
  }
  return rows;
}

function protoApiModel(root = repoRoot) {
  const protoRoot = resolve(root, 'proto/udb/core');
  const messages = new Map();
  const rpcs = [];
  let files = [];
  try {
    files = listFiles(protoRoot, '.proto');
  } catch {
    return { messages, rpcs };
  }

  function addMessage(pkg, name, record) {
    messages.set(`${pkg}.${name}`, record);
    if (!messages.has(name)) {
      messages.set(name, record);
    }
  }

  for (const path of files) {
    const text = readFileSync(path, 'utf8');
    const source = relative(root, path).replace(/\\/g, '/');
    const pkg = text.match(/\bpackage\s+([A-Za-z0-9_.]+)\s*;/)?.[1] || '';

    for (const messageMatch of text.matchAll(/\bmessage\s+([A-Za-z0-9_]+)\s*\{/g)) {
      const message = messageMatch[1];
      const openAt = messageMatch.index + messageMatch[0].lastIndexOf('{');
      const closeAt = findMatchingBrace(text, openAt);
      if (closeAt < 0) continue;
      const body = text.slice(openAt, closeAt + 1);
      const fields = parseProtoFields(body);
      addMessage(pkg, message, { pkg, message, source, fields });
    }

    for (const serviceMatch of text.matchAll(/\bservice\s+([A-Za-z0-9_]+)\s*\{/g)) {
      const service = serviceMatch[1];
      const serviceOpenAt = serviceMatch.index + serviceMatch[0].lastIndexOf('{');
      const serviceCloseAt = findMatchingBrace(text, serviceOpenAt);
      if (serviceCloseAt < 0) continue;
      const serviceBody = text.slice(serviceOpenAt, serviceCloseAt + 1);
      for (const rpcMatch of serviceBody.matchAll(/\brpc\s+([A-Za-z0-9_]+)\s*\(\s*([.A-Za-z0-9_]+)\s*\)\s*returns\s*\(\s*([.A-Za-z0-9_]+)\s*\)/g)) {
        const rpc = rpcMatch[1];
        const rpcStart = serviceOpenAt + rpcMatch.index;
        const rpcOpenAt = text.indexOf('{', rpcStart);
        const rpcCloseAt = rpcOpenAt < 0 ? -1 : findMatchingBrace(text, rpcOpenAt);
        const block = rpcOpenAt < 0 || rpcCloseAt < 0 ? '' : text.slice(rpcStart, rpcCloseAt + 1);
        const route = block.match(/\b(get|post|put|patch|delete)\s*:\s*"([^"]+)"/);
        rpcs.push({
          service: `${pkg}.${service}`,
          rpc,
          requestType: rpcMatch[2].replace(/^\./, ''),
          responseType: rpcMatch[3].replace(/^\./, ''),
          method: route?.[1]?.toUpperCase() || '',
          path: route?.[2] || '',
          body: block.match(/\bbody\s*:\s*"([^"]+)"/)?.[1] || '',
          source,
          hasHttp: Boolean(route),
        });
      }
    }
  }

  return { messages, rpcs };
}

function parseProtoFields(body) {
  const fields = [];
  const pendingComments = [];
  for (const line of String(body || '').split(/\r?\n/)) {
    const comment = line.match(/^\s*\/\/\s?(.*)$/);
    if (comment) {
      pendingComments.push(comment[1]);
      continue;
    }
    const fieldMatch = line.match(/^\s*(?:optional\s+)?(repeated\s+)?([.A-Za-z0-9_<>]+)\s+([A-Za-z0-9_]+)\s*=\s*\d+/);
    if (fieldMatch) {
      const inlineComment = line.match(/\/\/\s?(.*)$/)?.[1] || '';
      fields.push({
        repeated: Boolean(fieldMatch[1]),
        type: fieldMatch[2],
        name: fieldMatch[3],
        comments: [...pendingComments, inlineComment].filter(Boolean).join(' '),
      });
      pendingComments.length = 0;
      continue;
    }
    if (line.trim()) {
      pendingComments.length = 0;
    }
  }
  return fields;
}

function resolveMessage(model, typeName, pkg) {
  const normalized = String(typeName || '').replace(/^\./, '');
  return (
    model.messages.get(normalized) ||
    model.messages.get(`${pkg}.${normalized}`) ||
    model.messages.get(normalized.split('.').at(-1))
  );
}

function hasField(fields, name) {
  return fields.some((field) => field.name === name);
}

function hasFieldType(fields, pattern) {
  return fields.some((field) => pattern.test(field.type));
}

function fieldByName(fields, name) {
  return fields.find((field) => field.name === name);
}

function pathParameters(path) {
  const out = [];
  for (const match of String(path || '').matchAll(/\{([^}=]+)(?:=[^}]+)?\}/g)) {
    out.push(match[1]);
  }
  return out;
}

function routeResourceFieldNames(path) {
  const names = new Set();
  const valueObjectSegments = new Set(['stats', 'status', 'metrics', 'summary']);
  for (const segment of literalSegments(path)) {
    if (segment === 'v1' || segment.includes(':')) continue;
    const base = splitAction(segment).base.replace(/-/g, '_');
    if (!base || base === '.well_known') continue;
    if (valueObjectSegments.has(base)) continue;
    names.add(base);
    if (base.endsWith('ies')) {
      names.add(`${base.slice(0, -3)}y`);
    } else if (base.endsWith('s') && base.length > 1) {
      names.add(base.slice(0, -1));
    }
  }
  return names;
}

function isScalarType(type) {
  return /^(double|float|int32|int64|uint32|uint64|sint32|sint64|fixed32|fixed64|sfixed32|sfixed64|bool|string|bytes)$/.test(String(type || '').replace(/^\./, ''));
}

function isCommonEnvelopeType(type) {
  return /(?:^|\.)(ApiError|PageRequest|PageResponse|PaginationMeta|RequestContext|WriteReceipt|ReadFence|Status|Any|Timestamp|FieldMask)$/.test(String(type || ''));
}

function identityFieldNames(message) {
  const fields = message?.fields || [];
  return fields
    .filter((field) => field.name === 'name' || field.name === 'id' || field.name.endsWith('_id') || hasIdentityDocumentation(field))
    .map((field) => field.name);
}

function hasIdentityDocumentation(field) {
  return /\b(stable|canonical|public|opaque|uuid|ulid|format|server[- ]assigned|caller[- ]chosen|resource id|resource name)\b/i.test(String(field?.comments || ''));
}

function isLikelyParentOrAuthorityId(name) {
  return [
    'tenant_id',
    'project_id',
    'user_id',
    'owner_id',
    'actor_id',
    'subject_id',
    'provider_id',
    'parent_id',
    'parent_tenant_id',
    'request_id',
    'correlation_id',
    'reference_id',
    'resource_id',
  ].includes(name);
}

function paginationFlags(route, requestMessage, responseMessage) {
  const flags = [];
  const requestFields = requestMessage?.fields || [];
  const responseFields = responseMessage?.fields || [];
  const hasTokenRequest =
    (hasField(requestFields, 'page_size') && hasField(requestFields, 'page_token')) ||
    hasFieldType(requestFields, /(?:^|\.)PageRequest$/);
  const hasLegacyOffsetRequest =
    hasField(requestFields, 'page') && hasField(requestFields, 'page_size') && !hasField(requestFields, 'page_token');
  const hasTokenResponse =
    hasField(responseFields, 'next_page_token') ||
    hasFieldType(responseFields, /(?:^|\.)(PageResponse|PaginationMeta)$/);

  if (!hasTokenRequest) {
    flags.push({
      code: hasLegacyOffsetRequest ? 'legacy_offset_pagination' : 'missing_request_pagination',
      message: hasLegacyOffsetRequest
        ? 'list/search request uses legacy page/page_size without opaque page_token'
        : 'list/search request must expose page_size plus page_token or shared PageRequest',
    });
  }
  if (!hasTokenResponse) {
    flags.push({
      code: 'missing_response_pagination',
      message: 'list/search response must expose next_page_token or shared PageResponse',
    });
  }
  return flags;
}

function paginationContractRows(root = repoRoot) {
  const model = protoApiModel(root);
  const out = [];
  for (const rpc of model.rpcs) {
    if (!rpc.hasHttp || !/^(List|Search)/.test(rpc.rpc)) continue;
    const pkg = rpc.service.split('.').slice(0, -1).join('.');
    const requestMessage = resolveMessage(model, rpc.requestType, pkg);
    const responseMessage = resolveMessage(model, rpc.responseType, pkg);
    for (const flag of paginationFlags(rpc, requestMessage, responseMessage)) {
      out.push({
        rule: flag.code,
        category: 'pagination_contract',
        message: flag.message,
        service: rpc.service,
        rpc: rpc.rpc,
        method: rpc.method,
        path: rpc.path,
        source: rpc.source,
        requestType: rpc.requestType,
        responseType: rpc.responseType,
      });
    }
  }
  return out.sort((a, b) =>
    a.rule.localeCompare(b.rule) ||
    a.source.localeCompare(b.source) ||
    a.rpc.localeCompare(b.rpc));
}

function isScimProtocolRoute(route) {
  return /\/scim\//i.test(route.path);
}

function queryModifierDocumentation(field) {
  return String(field?.comments || '');
}

function hasAllowlistDocumentation(field) {
  return /\b(allowlist|allowed|valid|supported|one of|resource field|field path|SCIM|RFC\s*7644)\b/i.test(queryModifierDocumentation(field));
}

function queryUpdateContractRows(root = repoRoot) {
  const model = protoApiModel(root);
  const out = [];
  for (const rpc of model.rpcs) {
    if (!rpc.hasHttp) continue;
    const pkg = rpc.service.split('.').slice(0, -1).join('.');
    const requestMessage = resolveMessage(model, rpc.requestType, pkg);
    const requestFields = requestMessage?.fields || [];

    if (/^(List|Search)/.test(rpc.rpc)) {
      for (const field of requestFields) {
        if (!['filter', 'order_by', 'fields'].includes(field.name)) continue;
        if (field.name === 'filter' && isScimProtocolRoute(rpc)) continue;
        if (/sql|where clause/i.test(queryModifierDocumentation(field))) {
          out.push({
            rule: 'raw_sql_filter_exposure',
            category: 'query_update_contract',
            message: `query modifier "${field.name}" must not expose raw SQL or backend where clauses`,
            service: rpc.service,
            rpc: rpc.rpc,
            method: rpc.method,
            path: rpc.path,
            source: rpc.source,
            requestType: rpc.requestType,
            field: field.name,
          });
        }
        if (!hasAllowlistDocumentation(field)) {
          out.push({
            rule: `undocumented_${field.name}`,
            category: 'query_update_contract',
            message: `query modifier "${field.name}" must document or derive an allowlist`,
            service: rpc.service,
            rpc: rpc.rpc,
            method: rpc.method,
            path: rpc.path,
            source: rpc.source,
            requestType: rpc.requestType,
            field: field.name,
          });
        }
      }
    }

    if (rpc.method === 'PATCH' && !isScimProtocolRoute(rpc)) {
      const hasUpdateMask =
        hasField(requestFields, 'update_mask') ||
        hasFieldType(requestFields, /(?:^|\.)FieldMask$/);
      if (!hasUpdateMask) {
        out.push({
          rule: 'missing_update_mask',
          category: 'query_update_contract',
          message: 'PATCH/update RPC must use google.protobuf.FieldMask update_mask or document full-replace semantics',
          service: rpc.service,
          rpc: rpc.rpc,
          method: rpc.method,
          path: rpc.path,
          source: rpc.source,
          requestType: rpc.requestType,
          field: 'update_mask',
        });
      }
    }
  }
  return out.sort((a, b) =>
    a.rule.localeCompare(b.rule) ||
    a.source.localeCompare(b.source) ||
    a.rpc.localeCompare(b.rpc) ||
    a.field.localeCompare(b.field));
}

function resourceIdentityContractRows(root = repoRoot) {
  const model = protoApiModel(root);
  const out = [];
  for (const rpc of model.rpcs) {
    if (!rpc.hasHttp) continue;
    const pkg = rpc.service.split('.').slice(0, -1).join('.');
    const requestMessage = resolveMessage(model, rpc.requestType, pkg);
    const responseMessage = resolveMessage(model, rpc.responseType, pkg);
    const requestFields = requestMessage?.fields || [];
    const routeParams = pathParameters(rpc.path);

    for (const param of routeParams) {
      const field = fieldByName(requestFields, param);
      if (!field) {
        out.push({
          rule: 'missing_path_identity_field',
          category: 'resource_identity_contract',
          message: `path variable "{${param}}" must bind to a request field with the same canonical identity`,
          service: rpc.service,
          rpc: rpc.rpc,
          method: rpc.method,
          path: rpc.path,
          source: rpc.source,
          requestType: rpc.requestType,
          field: param,
        });
      }
      if (/(?:^|_)(row|db|database|pk|tuple|internal|physical)(?:_|$)/i.test(param)) {
        out.push({
          rule: 'physical_id_path_variable',
          category: 'resource_identity_contract',
          message: `path variable "{${param}}" looks like a backend physical identifier`,
          service: rpc.service,
          rpc: rpc.rpc,
          method: rpc.method,
          path: rpc.path,
          source: rpc.source,
          requestType: rpc.requestType,
          field: param,
        });
      }
    }

    if (String(rpc.path || '').split('/').includes('me')) {
      const identityFields = identityFieldNames(responseMessage);
      if (!identityFields.length) {
        out.push({
          rule: 'alias_without_canonical_identity',
          category: 'resource_identity_contract',
          message: 'alias route such as "me" must return a resource with canonical identity',
          service: rpc.service,
          rpc: rpc.rpc,
          method: rpc.method,
          path: rpc.path,
          source: rpc.source,
          requestType: rpc.requestType,
          field: 'me',
        });
      }
    }

    if (/^(Create|Register)/.test(rpc.rpc)) {
      for (const field of requestFields) {
        if (!field.name.endsWith('_id') || isLikelyParentOrAuthorityId(field.name)) continue;
        if (!hasIdentityDocumentation(field)) {
          out.push({
            rule: 'user_chosen_id_format_undocumented',
            category: 'resource_identity_contract',
            message: `caller-supplied identity field "${field.name}" must document allowed format or server-assigned semantics`,
            service: rpc.service,
            rpc: rpc.rpc,
            method: rpc.method,
            path: rpc.path,
            source: rpc.source,
            requestType: rpc.requestType,
            field: field.name,
          });
        }
      }
    }

    for (const field of responseMessage?.fields || []) {
      if (isScalarType(field.type) || isCommonEnvelopeType(field.type)) continue;
      if (!routeResourceFieldNames(rpc.path).has(field.name)) continue;
      const resourceMessage = resolveMessage(model, field.type, responseMessage.pkg);
      if (!resourceMessage || resourceMessage.message === responseMessage.message) continue;
      if (!identityFieldNames(resourceMessage).length) {
        out.push({
          rule: 'response_resource_missing_identity',
          category: 'resource_identity_contract',
          message: `response resource field "${field.name}" must carry a canonical identity field`,
          service: rpc.service,
          rpc: rpc.rpc,
          method: rpc.method,
          path: rpc.path,
          source: rpc.source,
          requestType: rpc.requestType,
          field: field.name,
        });
      }
    }
  }
  return out.sort((a, b) =>
    a.rule.localeCompare(b.rule) ||
    a.source.localeCompare(b.source) ||
    a.rpc.localeCompare(b.rpc) ||
    a.field.localeCompare(b.field));
}

function checkRepo(root = repoRoot, options = {}) {
  const allow = compileAllowlist(readJson(resolve(root, 'scripts/http-api-style.allow.json')));
  const rows = options.sourceOnly ? [] : inventory(root);
  const protoRows = protoHttpInventory(root);
  const violations = [];
  const seenViolations = new Set();
  for (const row of [...rows, ...protoRows]) {
    const flags = routeFlags({ verb: row.method, path: row.path, rpc: row.rpc }, allow);
    row.reasonFlags = flags.map((flag) => flag.code);
    for (const flag of flags) {
      const violation = `${row.source || row.service}::${row.rpc} ${row.method} ${row.path}: ${flag.message}`;
      if (!seenViolations.has(violation)) {
        violations.push(violation);
        seenViolations.add(violation);
      }
    }
  }
  if (!options.sourceOnly && rows.length > 0 && protoRows.length > 0 && rows.length !== protoRows.length) {
    violations.push(`route inventory mismatch: native-contract HTTP operations=${rows.length}, proto HTTP annotations=${protoRows.length}`);
  }
  return { rows, protoRows, violations };
}

function flagCategory(code) {
  if (['snake_case_literal', 'non_kebab_literal', 'singular_collection', 'bad_prefix', 'trailing_slash'].includes(code)) {
    return 'route_casing_and_shape';
  }
  if (['slash_verb', 'slash_read_action', 'pseudo_read_action', 'non_resource_command', 'bad_action_case'].includes(code)) {
    return 'action_style';
  }
  if (code === 'deep_path_review') return 'deep_path_review';
  return 'other';
}

function operationIdCollisionRows(rows) {
  const modes = [
    ['exact', (value) => String(value || '')],
    ['sdk_normalized', (value) => String(value || '').toLowerCase().replace(/[^a-z0-9]/g, '')],
    ['snake_case', snake],
    ['lowerCamel', lowerCamel],
    ['PascalCase', pascal],
  ];
  const collisions = [];
  for (const [mode, normalize] of modes) {
    const seen = new Map();
    for (const row of rows) {
      if (!row.operationId) continue;
      const key = normalize(row.operationId);
      const previous = seen.get(key);
      if (previous && previous.operationId !== row.operationId) {
        collisions.push({
          mode,
          normalized: key,
          first: {
            operationId: previous.operationId,
            service: previous.service,
            rpc: previous.rpc,
            path: previous.path,
          },
          second: {
            operationId: row.operationId,
            service: row.service,
            rpc: row.rpc,
            path: row.path,
          },
        });
      } else {
        seen.set(key, row);
      }
    }
  }
  return collisions;
}

function reportRows(rows, allow) {
  const out = [];
  for (const row of rows) {
    for (const flag of routeFlags({ verb: row.method, path: row.path, rpc: row.rpc }, allow)) {
      out.push({
        rule: flag.code,
        category: flagCategory(flag.code),
        message: flag.message,
        service: row.service,
        rpc: row.rpc,
        method: row.method,
        path: row.path,
        source: row.source,
        operationId: row.operationId,
      });
    }
  }
  return out.sort((a, b) =>
    a.category.localeCompare(b.category) ||
    a.rule.localeCompare(b.rule) ||
    a.source.localeCompare(b.source) ||
    a.rpc.localeCompare(b.rpc));
}

function groupBy(items, keyFn) {
  const grouped = {};
  for (const item of items) {
    const key = keyFn(item);
    grouped[key] = grouped[key] || [];
    grouped[key].push(item);
  }
  return grouped;
}

function buildExceptionReport(root = repoRoot) {
  const allowRaw = readJson(resolve(root, 'scripts/http-api-style.allow.json'));
  const allow = compileAllowlist(allowRaw);
  const result = checkRepo(root);
  const sourceResult = checkRepo(root, { sourceOnly: true });
  const routeExceptions = reportRows(result.rows, allow);
  const sourceRouteExceptions = reportRows(sourceResult.protoRows, allow);
  const operationIdCollisions = operationIdCollisionRows(result.rows);
  const resourceIdentityExceptions = resourceIdentityContractRows(root);
  const paginationExceptions = paginationContractRows(root);
  const queryUpdateExceptions = queryUpdateContractRows(root);
  return {
    generated_by: 'node scripts/check-http-api-style.mjs --write-report',
    counts: {
      native_contract_http_operations: result.rows.length,
      proto_http_annotations: sourceResult.protoRows.length,
      generated_route_style_exceptions: routeExceptions.length,
      source_route_style_exceptions: sourceRouteExceptions.length,
      resource_identity_contract_exceptions: resourceIdentityExceptions.length,
      pagination_contract_exceptions: paginationExceptions.length,
      query_update_contract_exceptions: queryUpdateExceptions.length,
      operation_id_collisions: operationIdCollisions.length,
      allowlist_entries:
        (allowRaw.allowedLiteralSegments || []).length +
        (allowRaw.allowedDeepPaths || []).length +
        (allowRaw.allowedCommandEndpoints || []).length,
    },
    allowlist_exceptions: {
      protocol_literals: allowRaw.allowedLiteralSegments || [],
      deep_paths: allowRaw.allowedDeepPaths || [],
      command_endpoints: allowRaw.allowedCommandEndpoints || [],
    },
    route_style_exceptions_by_rule: groupBy(routeExceptions, (item) => item.rule),
    source_route_style_exceptions_by_rule: groupBy(sourceRouteExceptions, (item) => item.rule),
    resource_identity_contract_exceptions_by_rule: groupBy(resourceIdentityExceptions, (item) => item.rule),
    pagination_contract_exceptions_by_rule: groupBy(paginationExceptions, (item) => item.rule),
    query_update_contract_exceptions_by_rule: groupBy(queryUpdateExceptions, (item) => item.rule),
    operation_id_collisions: operationIdCollisions,
    not_yet_reported_by_this_guard: [
      'REST boundary status/content-type behavior remains tracked by 14.8.6',
      'retry-safe mutation proof remains tracked by 14.8.8',
    ],
  };
}

function markdownTable(rows) {
  if (!rows.length) return '_None._\n';
  const lines = [
    '| Rule | RPC | Method | Path | Source | Message |',
    '| --- | --- | --- | --- | --- | --- |',
  ];
  for (const row of rows) {
    lines.push(`| ${row.rule} | ${row.service}/${row.rpc} | ${row.method} | \`${row.path}\` | \`${row.source || '-'}\` | ${row.message.replace(/\|/g, '\\|')} |`);
  }
  return `${lines.join('\n')}\n`;
}

function renderMarkdownReport(report) {
  const sourceRows = Object.values(report.source_route_style_exceptions_by_rule).flat();
  const generatedRows = Object.values(report.route_style_exceptions_by_rule).flat();
  const resourceIdentityRows = Object.values(report.resource_identity_contract_exceptions_by_rule).flat();
  const paginationRows = Object.values(report.pagination_contract_exceptions_by_rule).flat();
  const queryUpdateRows = Object.values(report.query_update_contract_exceptions_by_rule).flat();
  const collisions = report.operation_id_collisions;
  const lines = [
    '# HTTP API Style Exception Report',
    '',
    'Generated by `node scripts/check-http-api-style.mjs --write-report`.',
    '',
    '## Counts',
    '',
    `- Native-contract HTTP operations: ${report.counts.native_contract_http_operations}`,
    `- Proto HTTP annotations: ${report.counts.proto_http_annotations}`,
    `- Generated route-style exceptions: ${report.counts.generated_route_style_exceptions}`,
    `- Source route-style exceptions: ${report.counts.source_route_style_exceptions}`,
    `- Resource identity contract exceptions: ${report.counts.resource_identity_contract_exceptions}`,
    `- Pagination contract exceptions: ${report.counts.pagination_contract_exceptions}`,
    `- Query/update contract exceptions: ${report.counts.query_update_contract_exceptions}`,
    `- Operation ID collisions: ${report.counts.operation_id_collisions}`,
    `- Allowlist entries: ${report.counts.allowlist_entries}`,
    '',
    '## Source Route-Style Exceptions',
    '',
    markdownTable(sourceRows),
    '## Generated Route-Style Exceptions',
    '',
    markdownTable(generatedRows),
    '## Resource Identity Contract Exceptions',
    '',
    markdownTable(resourceIdentityRows),
    '## Pagination Contract Exceptions',
    '',
    markdownTable(paginationRows),
    '## Query/Update Contract Exceptions',
    '',
    markdownTable(queryUpdateRows),
    '## Operation ID Collisions',
    '',
  ];
  if (collisions.length) {
    for (const collision of collisions) {
      lines.push(`- ${collision.mode}: \`${collision.first.operationId}\` and \`${collision.second.operationId}\` both normalize to \`${collision.normalized}\`.`);
    }
  } else {
    lines.push('_None._');
  }
  lines.push('', '## Allowlist Exceptions', '');
  for (const [name, entries] of Object.entries(report.allowlist_exceptions)) {
    lines.push(`### ${name}`, '');
    if (!entries.length) {
      lines.push('_None._', '');
      continue;
    }
    for (const entry of entries) {
      lines.push(`- \`${entry.pathPattern}\` — ${entry.reason}`);
    }
    lines.push('');
  }
  lines.push('## Not Yet Reported By This Guard', '');
  for (const item of report.not_yet_reported_by_this_guard) {
    lines.push(`- ${item}`);
  }
  lines.push('');
  return lines.join('\n');
}

function writeExceptionReport(root = repoRoot) {
  const report = buildExceptionReport(root);
  const jsonPath = resolve(root, 'docs/generated/http-api-style-exceptions.json');
  const mdPath = resolve(root, 'docs/generated/http-api-style-exceptions.md');
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(mdPath, renderMarkdownReport(report));
  return { jsonPath, mdPath, report };
}

function writeFixture(root, path) {
  mkdirSync(resolve(root, 'scripts'), { recursive: true });
  writeFileSync(resolve(root, 'scripts/http-api-style.allow.json'), JSON.stringify({
    allowedLiteralSegments: [
      { pathPattern: '^/\\.well-known/jwks\\.json$', reason: 'well-known' },
      { pathPattern: '^/v1/auth/\\.well-known/jwks\\.json$', reason: 'auth well-known' },
      { pathPattern: '^/v1/idp/scim/[^/]+/(Users|Groups)(/.*)?$', reason: 'SCIM' },
    ],
    allowedDeepPaths: [],
    allowedCommandEndpoints: [],
  }, null, 2));
  mkdirSync(resolve(root, 'docs/generated'), { recursive: true });
  mkdirSync(resolve(root, 'proto/udb/core/test/services/v1'), { recursive: true });
  writeFileSync(resolve(root, 'proto/udb/core/test/services/v1/test_service.proto'), `
syntax = "proto3";
package udb.core.test.services.v1;
service TestService {
  rpc Sample(SampleRequest) returns (SampleResponse) {
    option (google.api.http) = { get: "${path}" };
  }
}
`.trimStart());
  writeFileSync(resolve(root, 'docs/generated/udb-native-contract.json'), JSON.stringify({
    services: [{
      service: 'udb.core.test.services.v1.TestService',
      rpcs: [{
        method: 'Sample',
        http: { verb: 'get', path },
        sdk_surface: { rest_operation_id: 'sample' },
      }],
    }],
  }, null, 2));
}

function writePaginationFixture(root, requestFields, responseFields) {
  mkdirSync(resolve(root, 'scripts'), { recursive: true });
  writeFileSync(resolve(root, 'scripts/http-api-style.allow.json'), JSON.stringify({
    allowedLiteralSegments: [],
    allowedDeepPaths: [],
    allowedCommandEndpoints: [],
  }, null, 2));
  mkdirSync(resolve(root, 'docs/generated'), { recursive: true });
  mkdirSync(resolve(root, 'proto/udb/core/test/services/v1'), { recursive: true });
  writeFileSync(resolve(root, 'proto/udb/core/test/services/v1/test_service.proto'), `
syntax = "proto3";
package udb.core.test.services.v1;

message PageRequest {
  int32 page_size = 1;
  string page_token = 2;
}

message PageResponse {
  string next_page_token = 1;
}

message Widget {
  string widget_id = 1;
}

message ListWidgetsRequest {
${requestFields}
}

message ListWidgetsResponse {
${responseFields}
}

service TestService {
  rpc ListWidgets(ListWidgetsRequest) returns (ListWidgetsResponse) {
    option (google.api.http) = { get: "/v1/widgets" };
  }
}
`.trimStart());
  writeFileSync(resolve(root, 'docs/generated/udb-native-contract.json'), JSON.stringify({
    services: [{
      service: 'udb.core.test.services.v1.TestService',
      rpcs: [{
        method: 'ListWidgets',
        http: { verb: 'get', path: '/v1/widgets' },
        sdk_surface: { rest_operation_id: 'listWidgets' },
      }],
    }],
  }, null, 2));
}

function writeQueryUpdateFixture(root, requestFields, responseFields, route = 'get: "/v1/widgets"', rpc = 'ListWidgets') {
  mkdirSync(resolve(root, 'scripts'), { recursive: true });
  writeFileSync(resolve(root, 'scripts/http-api-style.allow.json'), JSON.stringify({
    allowedLiteralSegments: [],
    allowedDeepPaths: [],
    allowedCommandEndpoints: [],
  }, null, 2));
  mkdirSync(resolve(root, 'docs/generated'), { recursive: true });
  mkdirSync(resolve(root, 'proto/udb/core/test/services/v1'), { recursive: true });
  const requestName = `${rpc}Request`;
  const responseName = `${rpc}Response`;
  writeFileSync(resolve(root, 'proto/udb/core/test/services/v1/test_service.proto'), `
syntax = "proto3";
package udb.core.test.services.v1;

message Widget {
  string widget_id = 1;
}

message ${requestName} {
${requestFields}
}

message ${responseName} {
${responseFields}
}

service TestService {
  rpc ${rpc}(${requestName}) returns (${responseName}) {
    option (google.api.http) = { ${route} };
  }
}
`.trimStart());
  writeFileSync(resolve(root, 'docs/generated/udb-native-contract.json'), JSON.stringify({
    services: [{
      service: 'udb.core.test.services.v1.TestService',
      rpcs: [{
        method: rpc,
        http: { verb: route.split(':')[0].trim(), path: route.match(/"([^"]+)"/)?.[1] || '/v1/widgets' },
        sdk_surface: { rest_operation_id: lowerCamel(rpc) },
      }],
    }],
  }, null, 2));
}

function writeResourceIdentityFixture(root, requestFields, responseFields, route = 'get: "/v1/widgets/{widget_id}"', rpc = 'GetWidget') {
  mkdirSync(resolve(root, 'scripts'), { recursive: true });
  writeFileSync(resolve(root, 'scripts/http-api-style.allow.json'), JSON.stringify({
    allowedLiteralSegments: [],
    allowedDeepPaths: [],
    allowedCommandEndpoints: [],
  }, null, 2));
  mkdirSync(resolve(root, 'docs/generated'), { recursive: true });
  mkdirSync(resolve(root, 'proto/udb/core/test/services/v1'), { recursive: true });
  const requestName = `${rpc}Request`;
  const responseName = `${rpc}Response`;
  writeFileSync(resolve(root, 'proto/udb/core/test/services/v1/test_service.proto'), `
syntax = "proto3";
package udb.core.test.services.v1;

message Widget {
  string widget_id = 1;
}

message NamelessWidget {
  string display_name = 1;
}

message ${requestName} {
${requestFields}
}

message ${responseName} {
${responseFields}
}

service TestService {
  rpc ${rpc}(${requestName}) returns (${responseName}) {
    option (google.api.http) = { ${route} };
  }
}
`.trimStart());
  writeFileSync(resolve(root, 'docs/generated/udb-native-contract.json'), JSON.stringify({
    services: [{
      service: 'udb.core.test.services.v1.TestService',
      rpcs: [{
        method: rpc,
        http: { verb: route.split(':')[0].trim(), path: route.match(/"([^"]+)"/)?.[1] || '/v1/widgets/{widget_id}' },
        sdk_surface: { rest_operation_id: lowerCamel(rpc) },
      }],
    }],
  }, null, 2));
}

function assertSelftest(condition, message) {
  if (!condition) throw new Error(message);
}

function runSelftest() {
  const root = mkdtempSync(resolve(tmpdir(), 'udb-http-style-'));
  try {
    writeFixture(root, '/v1/widgets/{widget_id}:refreshState');
    let result = checkRepo(root);
    assertSelftest(result.rows.length === 1, 'inventory did not report fixture operation');
    assertSelftest(result.rows[0].source === 'proto/udb/core/test/services/v1/test_service.proto', 'inventory did not attach source proto');
    assertSelftest(result.violations.length === 0, `good route failed:\n${result.violations.join('\n')}`);

    writeFixture(root, '/.well-known/jwks.json');
    result = checkRepo(root);
    assertSelftest(result.violations.length === 0, `JWKS allowlist failed:\n${result.violations.join('\n')}`);

    writeFixture(root, '/v1/idp/scim/provider/Users/{scim_user_id}');
    result = checkRepo(root);
    assertSelftest(result.violations.length === 0, `SCIM allowlist failed:\n${result.violations.join('\n')}`);

    writeFixture(root, '/v1/api_keys');
    result = checkRepo(root);
    assertSelftest(result.violations.some((line) => line.includes('snake_case')), 'api_keys snake_case regression was not caught');

    writeFixture(root, '/v1/storage/uploads/{file_id}/finalize');
    result = checkRepo(root);
    assertSelftest(result.violations.some((line) => line.includes('slash verb')), 'slash finalize regression was not caught');

    writeFixture(root, '/v1/storage/files/{file_id}/download-url');
    result = checkRepo(root);
    assertSelftest(result.violations.some((line) => line.includes('slash read action')), 'slash download-url regression was not caught');

    writeFixture(root, '/v1/widgets:list');
    result = checkRepo(root);
    assertSelftest(result.violations.some((line) => line.includes('pseudo')), 'pseudo-read action regression was not caught');

    const report = buildExceptionReport(root);
    assertSelftest(report.counts.proto_http_annotations === 1, 'report proto count is wrong');
    assertSelftest(report.source_route_style_exceptions_by_rule.pseudo_read_action?.length === 1, 'report did not group pseudo-read exception');

    writePaginationFixture(root, '  PageRequest page = 1;', '  repeated Widget widgets = 1;\n  PageResponse page = 2;');
    result = checkRepo(root);
    assertSelftest(result.violations.length === 0, `good pagination route fixture failed route style:\n${result.violations.join('\n')}`);
    let paginationRows = paginationContractRows(root);
    assertSelftest(paginationRows.length === 0, `good pagination fixture failed:\n${JSON.stringify(paginationRows, null, 2)}`);

    writePaginationFixture(root, '  int32 page = 1;\n  int32 page_size = 2;', '  repeated Widget widgets = 1;\n  int32 total_count = 2;');
    paginationRows = paginationContractRows(root);
    assertSelftest(paginationRows.some((row) => row.rule === 'legacy_offset_pagination'), 'legacy offset pagination regression was not caught');
    assertSelftest(paginationRows.some((row) => row.rule === 'missing_response_pagination'), 'missing next_page_token regression was not caught');

    writeQueryUpdateFixture(root, '  // Allowed filter fields: status, owner.\n  string filter = 1;', '  repeated Widget widgets = 1;');
    let queryUpdateRows = queryUpdateContractRows(root);
    assertSelftest(queryUpdateRows.length === 0, `documented filter fixture failed:\n${JSON.stringify(queryUpdateRows, null, 2)}`);

    writeQueryUpdateFixture(root, '  string filter = 1;', '  repeated Widget widgets = 1;');
    queryUpdateRows = queryUpdateContractRows(root);
    assertSelftest(queryUpdateRows.some((row) => row.rule === 'undocumented_filter'), 'undocumented filter regression was not caught');

    writeQueryUpdateFixture(root, '  string widget_id = 1;\n  string display_name = 2;', '  Widget widget = 1;', 'patch: "/v1/widgets/{widget_id}" body: "*"', 'UpdateWidget');
    queryUpdateRows = queryUpdateContractRows(root);
    assertSelftest(queryUpdateRows.some((row) => row.rule === 'missing_update_mask'), 'missing update_mask regression was not caught');

    writeResourceIdentityFixture(root, '  string widget_id = 1;', '  Widget widget = 1;');
    let resourceIdentityRows = resourceIdentityContractRows(root);
    assertSelftest(resourceIdentityRows.length === 0, `good resource identity fixture failed:\n${JSON.stringify(resourceIdentityRows, null, 2)}`);

    writeResourceIdentityFixture(root, '  string id = 1;', '  Widget widget = 1;');
    resourceIdentityRows = resourceIdentityContractRows(root);
    assertSelftest(resourceIdentityRows.some((row) => row.rule === 'missing_path_identity_field'), 'missing path identity field regression was not caught');

    writeResourceIdentityFixture(root, '  string widget_id = 1;', '  NamelessWidget widget = 1;');
    resourceIdentityRows = resourceIdentityContractRows(root);
    assertSelftest(resourceIdentityRows.some((row) => row.rule === 'response_resource_missing_identity'), 'missing response identity regression was not caught');

    writeResourceIdentityFixture(root, '  string widget_id = 1;', '  Widget widget = 1;', 'post: "/v1/widgets"', 'CreateWidget');
    resourceIdentityRows = resourceIdentityContractRows(root);
    assertSelftest(resourceIdentityRows.some((row) => row.rule === 'user_chosen_id_format_undocumented'), 'undocumented user-chosen ID regression was not caught');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
  console.log('HTTP API style selftest passed');
}

function main() {
  const args = new Set(process.argv.slice(2));
  if (args.has('--selftest')) {
    runSelftest();
    return;
  }
  if (args.has('--write-report')) {
    const { jsonPath, mdPath, report } = writeExceptionReport(repoRoot);
    console.log(`HTTP API style exception report: ${report.counts.source_route_style_exceptions} source exception(s), ${report.counts.operation_id_collisions} operationId collision(s)`);
    console.log(`wrote ${relative(repoRoot, jsonPath).replace(/\\/g, '/')}`);
    console.log(`wrote ${relative(repoRoot, mdPath).replace(/\\/g, '/')}`);
    return;
  }
  if (args.has('--pagination-contract')) {
    const rows = paginationContractRows(repoRoot);
    if (args.has('--inventory')) {
      console.log(JSON.stringify(rows, null, 2));
    }
    if (rows.length) {
      for (const row of rows) {
        const line = `${row.source}::${row.rpc} ${row.method} ${row.path}: ${row.message}`;
        if (args.has('--advisory')) {
          console.warn(`::warning::${line}`);
        } else {
          console.error(line);
        }
      }
      if (!args.has('--advisory')) {
        process.exit(1);
      }
    }
    console.log(`HTTP API pagination contract inventory: ${rows.length} violation(s)`);
    return;
  }
  if (args.has('--resource-identity-contract')) {
    const rows = resourceIdentityContractRows(repoRoot);
    if (args.has('--inventory')) {
      console.log(JSON.stringify(rows, null, 2));
    }
    if (rows.length) {
      for (const row of rows) {
        const line = `${row.source}::${row.rpc} ${row.method} ${row.path}: ${row.message}`;
        if (args.has('--advisory')) {
          console.warn(`::warning::${line}`);
        } else {
          console.error(line);
        }
      }
      if (!args.has('--advisory')) {
        process.exit(1);
      }
    }
    console.log(`HTTP API resource identity contract inventory: ${rows.length} violation(s)`);
    return;
  }
  if (args.has('--query-update-contract')) {
    const rows = queryUpdateContractRows(repoRoot);
    if (args.has('--inventory')) {
      console.log(JSON.stringify(rows, null, 2));
    }
    if (rows.length) {
      for (const row of rows) {
        const line = `${row.source}::${row.rpc} ${row.method} ${row.path}: ${row.message}`;
        if (args.has('--advisory')) {
          console.warn(`::warning::${line}`);
        } else {
          console.error(line);
        }
      }
      if (!args.has('--advisory')) {
        process.exit(1);
      }
    }
    console.log(`HTTP API query/update contract inventory: ${rows.length} violation(s)`);
    return;
  }
  const result = checkRepo(repoRoot, { sourceOnly: args.has('--source-only') });
  const displayedRows = args.has('--source-only') ? result.protoRows : result.rows;
  if (args.has('--inventory')) {
    console.log(JSON.stringify(displayedRows, null, 2));
  }
  if (result.violations.length) {
    for (const violation of result.violations) {
      console.error(`::warning::${violation}`);
    }
    if (!args.has('--advisory')) {
      process.exit(1);
    }
  }
  if (!args.has('--inventory')) {
    const label = args.has('--source-only') ? 'source HTTP API style inventory' : 'HTTP API style inventory';
    console.log(`${label}: ${displayedRows.length} operation(s), ${result.violations.length} violation(s)`);
  }
}

main();
