"use strict";
// The broker's filter grammar and value comparison, for the in-memory broker.
Object.defineProperty(exports, "__esModule", { value: true });
exports.equal = equal;
exports.compare = compare;
exports.validateFilter = validateFilter;
exports.matches = matches;
exports.holds = holds;
const grpc_js_1 = require("@grpc/grpc-js");
const recordCodec_1 = require("../recordCodec");
const refusal_1 = require("./refusal");
const SUPPORTED = new Set(["$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$between", "$is_null", "$not_null", "$not", "$like", "$ilike"]);
function isObject(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function numberOf(value) {
    if (typeof value === "number" || typeof value === "bigint")
        return value;
    return undefined;
}
function numberFromText(value) {
    if (typeof value !== "string")
        return undefined;
    const text = value.trim();
    if (/^[+-]?\d+$/.test(text))
        return BigInt(text);
    if (/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(text))
        return Number(text);
    return undefined;
}
function numberPair(a, b) {
    const left = numberOf(a);
    const right = numberOf(b);
    if (left !== undefined && right !== undefined)
        return [left, right];
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
function timePair(a, b) {
    if (typeof a !== "string" || typeof b !== "string")
        return undefined;
    const left = (0, recordCodec_1.parseRfc3339)(a);
    const right = (0, recordCodec_1.parseRfc3339)(b);
    if (!left || !right)
        return undefined;
    return Math.sign(left.seconds - right.seconds || left.nanos - right.nanos);
}
function jsonText(value) {
    return JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? item.toString() : item)) ?? "null";
}
function equal(a, b) {
    const pair = numberPair(a, b);
    if (pair)
        return pair[0] == pair[1]; // eslint-disable-line eqeqeq -- bigint with number compares exactly.
    const time = timePair(a, b);
    if (time !== undefined)
        return time === 0;
    if (typeof a === "string" && typeof b === "string")
        return a === b;
    return jsonText(a) === jsonText(b);
}
function compare(a, b) {
    const pair = numberPair(a, b);
    if (pair)
        return pair[0] < pair[1] ? -1 : pair[0] > pair[1] ? 1 : 0;
    const time = timePair(a, b);
    if (time !== undefined)
        return time;
    const left = a === null || a === undefined ? "<nil>" : String(a);
    const right = b === null || b === undefined ? "<nil>" : String(b);
    return left < right ? -1 : left > right ? 1 : 0;
}
/** Refuse the shapes the broker refuses at plan time, before any row is read: an
 *  unknown operator and an equality with NULL. */
function validateFilter(filter) {
    for (const [key, condition] of Object.entries(filter)) {
        const name = key.trim().toLowerCase();
        if (name === "$and" || name === "$or") {
            if (!Array.isArray(condition) || condition.length === 0) {
                throw (0, refusal_1.refuse)(grpc_js_1.status.INVALID_ARGUMENT, "", "boolean filter requires a nonempty array of objects");
            }
            for (const item of condition) {
                if (!isObject(item) || Object.keys(item).length === 0) {
                    throw (0, refusal_1.refuse)(grpc_js_1.status.INVALID_ARGUMENT, "", "boolean filter child must be a nonempty object");
                }
                validateFilter(item);
            }
            continue;
        }
        if (condition === null || condition === undefined) {
            throw (0, refusal_1.refuse)(grpc_js_1.status.INVALID_ARGUMENT, "UDB_NULL_COMPARISON", "comparison with NULL matches no rows; use $is_null");
        }
        validateOperators(condition);
    }
}
function validateOperators(condition) {
    if (!isObject(condition))
        return;
    for (const [op, arg] of Object.entries(condition)) {
        const name = op.toLowerCase();
        if (!SUPPORTED.has(name)) {
            throw (0, refusal_1.refuse)(grpc_js_1.status.INVALID_ARGUMENT, "UDB_UNSUPPORTED_FILTER_OPERATOR", `unsupported filter operator ${op}`);
        }
        if ((name === "$eq" || name === "$ne") && (arg === null || arg === undefined)) {
            throw (0, refusal_1.refuse)(grpc_js_1.status.INVALID_ARGUMENT, "UDB_NULL_COMPARISON", "use $is_null or $not_null instead of comparing NULL");
        }
        if (name === "$in" || name === "$nin" || name === "$between") {
            if (!Array.isArray(arg) || (name === "$between" && arg.length !== 2)) {
                throw (0, refusal_1.refuse)(grpc_js_1.status.INVALID_ARGUMENT, "", "filter operator requires an array of values");
            }
        }
        if (name === "$not")
            validateOperators(arg);
    }
}
function matches(row, filter) {
    for (const [key, condition] of Object.entries(filter)) {
        const name = key.toLowerCase();
        if (name === "$and") {
            if (!condition.every((item) => matches(row, item)))
                return false;
            continue;
        }
        if (name === "$or") {
            if (!condition.some((item) => matches(row, item)))
                return false;
            continue;
        }
        if (columnTruth(row[key] ?? null, condition) !== true)
            return false;
    }
    return true;
}
function likeMatch(text, pattern) {
    if (pattern === "")
        return text === "";
    if (pattern[0] === "%") {
        for (let i = 0; i <= text.length; i += 1) {
            if (likeMatch(text.slice(i), pattern.slice(1)))
                return true;
        }
        return false;
    }
    if (text === "")
        return false;
    if (pattern[0] === "_")
        return likeMatch(text.slice(1), pattern.slice(1));
    return text[0] === pattern[0] && likeMatch(text.slice(1), pattern.slice(1));
}
/** SQL's three-valued logic: NULL <> x and NOT(NULL = x) stay unknown (null) and
 *  cannot select a row, also through nested NOT predicates. */
function columnTruth(value, condition) {
    if (!isObject(condition)) {
        if (condition === null || condition === undefined) {
            throw (0, refusal_1.refuse)(grpc_js_1.status.INVALID_ARGUMENT, "UDB_NULL_COMPARISON", "comparison with NULL matches no rows; use $is_null");
        }
        if (value === null || value === undefined)
            return null;
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
                const items = arg ?? [];
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
                const bounds = arg;
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
            default:
                throw (0, refusal_1.refuse)(grpc_js_1.status.INVALID_ARGUMENT, "UDB_UNSUPPORTED_FILTER_OPERATOR", `unsupported filter operator ${op}`);
        }
        if (!ok)
            return false;
    }
    return unknown ? null : true;
}
/** Whether every expected column equals the row's value. */
function holds(row, expected) {
    return Object.entries(expected).every(([column, want]) => equal(row[column] ?? null, want));
}
