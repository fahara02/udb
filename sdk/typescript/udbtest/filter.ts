// The broker's filter grammar and value comparison, for the in-memory broker.

import { status as codes } from "@grpc/grpc-js";

import { parseRfc3339 } from "../recordCodec";
import { refuse } from "./refusal";

export type Row = Record<string, any>;

const SUPPORTED = new Set([
  "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$between", "$is_null", "$not_null", "$not", "$like", "$ilike",
  "$contains", "$contained_by", "$overlaps", "$overlap",
]);

/** The array/JSON operators. `$overlap` is an alias of `$overlaps`. */
export const ARRAY_OPERATORS = new Set(["$contains", "$contained_by", "$overlaps", "$overlap"]);

/** How the broker evaluates `$contains`/`$contained_by` on a JSON column: JSON
 *  containment (`@>`/`<@` on jsonb). Entity rules rewrite the operator to these
 *  internal names once the column is known to be JSON; callers can never send them. */
export const JSON_CONTAINS = "$contains:json";
export const JSON_CONTAINED_BY = "$contained_by:json";

function isScalar(value: unknown): boolean {
  return value === null || (typeof value !== "object" && !Array.isArray(value));
}

/** jsonb scalar equality: same JSON type, numbers compared by value. */
function jsonScalarEqual(a: unknown, b: unknown): boolean {
  const left = numberOf(a);
  const right = numberOf(b);
  if (left !== undefined && right !== undefined) return left == right; // eslint-disable-line eqeqeq -- bigint with number compares exactly.
  return a === b;
}

/** jsonb `container @> contained`: objects by key (recursively), arrays by
 *  element (each contained element is contained in some container element), and,
 *  at the top level only, an array contains a bare scalar it holds. */
export function jsonContains(container: unknown, contained: unknown, top = true): boolean {
  if (Array.isArray(container)) {
    if (Array.isArray(contained)) {
      return contained.every((want) => container.some((have) => jsonContains(have, want, false)));
    }
    if (top && isScalar(contained)) return container.some((have) => isScalar(have) && jsonScalarEqual(have, contained));
    return false;
  }
  if (isObject(container)) {
    if (!isObject(contained)) return false;
    return Object.entries(contained).every(([key, want]) => key in container && jsonContains(container[key], want, false));
  }
  if (!isScalar(contained)) return false;
  return jsonScalarEqual(container, contained);
}

/** SQL array element equality: a NULL element equals nothing. */
function elementEqual(a: unknown, b: unknown): boolean {
  if (a === null || a === undefined || b === null || b === undefined) return false;
  return equal(a, b);
}

/** SQL array `container @> contained`: every contained element appears in the container. */
export function arrayContains(container: unknown, contained: unknown): boolean {
  if (!Array.isArray(container) || !Array.isArray(contained)) return false;
  return contained.every((want) => container.some((have) => elementEqual(have, want)));
}

/** SQL array `a && b`: the arrays share an element. */
export function arrayOverlaps(a: unknown, b: unknown): boolean {
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  return a.some((have) => b.some((want) => elementEqual(have, want)));
}

function isObject(value: unknown): value is Record<string, any> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function numberOf(value: unknown): number | bigint | undefined {
  if (typeof value === "number" || typeof value === "bigint") return value;
  return undefined;
}

function numberFromText(value: unknown): number | bigint | undefined {
  if (typeof value !== "string") return undefined;
  const text = value.trim();
  if (/^[+-]?\d+$/.test(text)) return BigInt(text);
  if (/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(text)) return Number(text);
  return undefined;
}

function numberPair(a: unknown, b: unknown): [number | bigint, number | bigint] | undefined {
  const left = numberOf(a);
  const right = numberOf(b);
  if (left !== undefined && right !== undefined) return [left, right];
  if (left !== undefined) {
    const parsed = numberFromText(b);
    return parsed === undefined ? undefined : [left, parsed];
  }
  if (right !== undefined) {
    const parsed = numberFromText(a);
    return parsed === undefined ? undefined : [parsed, right];
  }
  return undefined;
}

function timePair(a: unknown, b: unknown): number | undefined {
  if (typeof a !== "string" || typeof b !== "string") return undefined;
  const left = parseRfc3339(a);
  const right = parseRfc3339(b);
  if (!left || !right) return undefined;
  return Math.sign(left.seconds - right.seconds || left.nanos - right.nanos);
}

function jsonText(value: unknown): string {
  return JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? item.toString() : item)) ?? "null";
}

export function equal(a: unknown, b: unknown): boolean {
  const pair = numberPair(a, b);
  if (pair) return pair[0] == pair[1]; // eslint-disable-line eqeqeq -- bigint with number compares exactly.
  const time = timePair(a, b);
  if (time !== undefined) return time === 0;
  if (typeof a === "string" && typeof b === "string") return a === b;
  return jsonText(a) === jsonText(b);
}

export function compare(a: unknown, b: unknown): number {
  const pair = numberPair(a, b);
  if (pair) return pair[0] < pair[1] ? -1 : pair[0] > pair[1] ? 1 : 0;
  const time = timePair(a, b);
  if (time !== undefined) return time;
  const left = a === null || a === undefined ? "<nil>" : String(a);
  const right = b === null || b === undefined ? "<nil>" : String(b);
  return left < right ? -1 : left > right ? 1 : 0;
}

/** Refuse the shapes the broker refuses at plan time, before any row is read: an
 *  unknown operator and an equality with NULL. */
export function validateFilter(filter: Row): void {
  for (const [key, condition] of Object.entries(filter)) {
    const name = key.trim().toLowerCase();
    if (name === "$and" || name === "$or") {
      if (!Array.isArray(condition) || condition.length === 0) {
        throw refuse(codes.INVALID_ARGUMENT, "", "boolean filter requires a nonempty array of objects");
      }
      for (const item of condition) {
        if (!isObject(item) || Object.keys(item).length === 0) {
          throw refuse(codes.INVALID_ARGUMENT, "", "boolean filter child must be a nonempty object");
        }
        validateFilter(item);
      }
      continue;
    }
    if (condition === null || condition === undefined) {
      throw refuse(codes.INVALID_ARGUMENT, "UDB_NULL_COMPARISON", "comparison with NULL matches no rows; use $is_null");
    }
    validateOperators(condition);
  }
}

function validateOperators(condition: unknown): void {
  if (!isObject(condition)) return;
  for (const [op, arg] of Object.entries(condition)) {
    const name = op.toLowerCase();
    if (!SUPPORTED.has(name)) {
      throw refuse(codes.INVALID_ARGUMENT, "UDB_UNSUPPORTED_FILTER_OPERATOR", `unsupported filter operator ${op}`);
    }
    if ((name === "$eq" || name === "$ne") && (arg === null || arg === undefined)) {
      throw refuse(codes.INVALID_ARGUMENT, "UDB_NULL_COMPARISON", "use $is_null or $not_null instead of comparing NULL");
    }
    if (name === "$in" || name === "$nin" || name === "$between") {
      if (!Array.isArray(arg) || (name === "$between" && arg.length !== 2)) {
        throw refuse(codes.INVALID_ARGUMENT, "", "filter operator requires an array of values");
      }
    }
    if (name === "$not") validateOperators(arg);
  }
}

export function matches(row: Row, filter: Row): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    const name = key.toLowerCase();
    if (name === "$and") {
      if (!(condition as Row[]).every((item) => matches(row, item))) return false;
      continue;
    }
    if (name === "$or") {
      if (!(condition as Row[]).some((item) => matches(row, item))) return false;
      continue;
    }
    if (columnTruth(row[key] ?? null, condition) !== true) return false;
  }
  return true;
}

function likeMatch(text: string, pattern: string): boolean {
  if (pattern === "") return text === "";
  if (pattern[0] === "%") {
    for (let i = 0; i <= text.length; i += 1) {
      if (likeMatch(text.slice(i), pattern.slice(1))) return true;
    }
    return false;
  }
  if (text === "") return false;
  if (pattern[0] === "_") return likeMatch(text.slice(1), pattern.slice(1));
  return text[0] === pattern[0] && likeMatch(text.slice(1), pattern.slice(1));
}

/** SQL's three-valued logic: NULL <> x and NOT(NULL = x) stay unknown (null) and
 *  cannot select a row, also through nested NOT predicates. */
function columnTruth(value: unknown, condition: unknown): boolean | null {
  if (!isObject(condition)) {
    if (condition === null || condition === undefined) {
      throw refuse(codes.INVALID_ARGUMENT, "UDB_NULL_COMPARISON", "comparison with NULL matches no rows; use $is_null");
    }
    if (value === null || value === undefined) return null;
    return equal(value, condition);
  }
  let unknown = false;
  const isNull = value === null || value === undefined;
  for (const [op, arg] of Object.entries(condition)) {
    const name = op.toLowerCase();
    if (!["$is_null", "$not_null", "$not", "$in", "$nin"].includes(name) && (isNull || arg === null || arg === undefined)) {
      unknown = true;
      continue;
    }
    let ok = false;
    switch (name) {
      case "$eq":
        ok = equal(value, arg);
        break;
      case "$ne":
        ok = !equal(value, arg);
        break;
      case "$gt":
        ok = compare(value, arg) > 0;
        break;
      case "$gte":
        ok = compare(value, arg) >= 0;
        break;
      case "$lt":
        ok = compare(value, arg) < 0;
        break;
      case "$lte":
        ok = compare(value, arg) <= 0;
        break;
      case "$in":
      case "$nin": {
        const items = (arg as unknown[]) ?? [];
        if (items.length === 0) {
          ok = name === "$nin";
          break;
        }
        if (isNull) {
          unknown = true;
          continue;
        }
        const found = items.some((item) => equal(value, item));
        if (!found && items.some((item) => item === null)) {
          unknown = true;
          continue;
        }
        ok = found === (name === "$in");
        break;
      }
      case "$between": {
        const bounds = arg as unknown[];
        if (bounds[0] === null || bounds[1] === null) {
          unknown = true;
          continue;
        }
        ok = compare(value, bounds[0]) >= 0 && compare(value, bounds[1]) <= 0;
        break;
      }
      case "$is_null":
        ok = isNull === (typeof arg === "boolean" ? arg : true);
        break;
      case "$not_null":
        ok = !isNull === (typeof arg === "boolean" ? arg : true);
        break;
      case "$not": {
        const inner = columnTruth(value, arg);
        if (inner === null) {
          unknown = true;
          continue;
        }
        ok = inner === false;
        break;
      }
      case "$like":
      case "$ilike": {
        let pattern = typeof arg === "string" ? arg : "";
        let text = typeof value === "string" ? value : "";
        if (name === "$ilike") {
          pattern = pattern.toLowerCase();
          text = text.toLowerCase();
        }
        ok = likeMatch(text, pattern);
        break;
      }
      case "$contains":
        ok = arrayContains(value, arg);
        break;
      case "$contained_by":
        ok = arrayContains(arg, value);
        break;
      case "$overlaps":
      case "$overlap":
        ok = arrayOverlaps(value, arg);
        break;
      case JSON_CONTAINS:
        ok = jsonContains(value, arg);
        break;
      case JSON_CONTAINED_BY:
        ok = jsonContains(arg, value);
        break;
      default:
        throw refuse(codes.INVALID_ARGUMENT, "UDB_UNSUPPORTED_FILTER_OPERATOR", `unsupported filter operator ${op}`);
    }
    if (!ok) return false;
  }
  return unknown ? null : true;
}

/** Whether every expected column equals the row's value. */
export function holds(row: Row, expected: Row): boolean {
  return Object.entries(expected).every(([column, want]) => equal(row[column] ?? null, want));
}
