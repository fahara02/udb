"use strict";
// The policy file schema `udb policy diff|apply` reads, as JSON or YAML. A file the
// real command would refuse is refused here too, so a test cannot pass on a policy
// that production rejects.
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.parsePolicies = parsePolicies;
exports.loadPolicies = loadPolicies;
const node_fs_1 = __importDefault(require("node:fs"));
const yaml_1 = require("yaml");
const TEXT_FIELDS = ["id", "effect", "tenant", "project", "subject", "role", "action", "resource", "purpose", "relationship"];
const YAML_TRUE = ["y", "yes", "on", "true"];
const YAML_FALSE = ["n", "no", "off", "false"];
function schemaError(message) {
    return new Error(`udbtest: policy schema: ${message}`);
}
function isMap(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
/** A scalar reads as text, as it does for a string field in the broker's decoder. */
function text(value, field) {
    if (value === null || value === undefined)
        return "";
    if (typeof value === "object")
        throw schemaError(`${field} must be a scalar`);
    return String(value);
}
function bool(value, field) {
    if (value === null || value === undefined)
        return undefined;
    if (typeof value === "boolean")
        return value;
    const word = typeof value === "string" ? value.toLowerCase() : "";
    if (YAML_TRUE.includes(word))
        return true;
    if (YAML_FALSE.includes(word))
        return false;
    throw schemaError(`${field} must be a boolean`);
}
function readPolicy(raw, index) {
    const where = `policies[${index}]`;
    if (!isMap(raw))
        throw schemaError(`${where} must be a mapping`);
    const policy = {};
    for (const field of TEXT_FIELDS) {
        if (field in raw)
            policy[field] = text(raw[field], `${where}.${field}`);
    }
    if (raw.priority !== null && raw.priority !== undefined) {
        if (!Number.isInteger(raw.priority))
            throw schemaError(`${where}.priority must be an integer`);
        policy.priority = raw.priority;
    }
    const enabled = bool(raw.enabled, `${where}.enabled`);
    if (enabled !== undefined)
        policy.enabled = enabled;
    if (raw.conditions !== null && raw.conditions !== undefined) {
        if (!isMap(raw.conditions))
            throw schemaError(`${where}.conditions must be a mapping`);
        policy.conditions = Object.fromEntries(Object.entries(raw.conditions).map(([key, value]) => [key, text(value, `${where}.conditions.${key}`)]));
    }
    if (raw.required_scopes !== null && raw.required_scopes !== undefined) {
        if (!Array.isArray(raw.required_scopes))
            throw schemaError(`${where}.required_scopes must be a list`);
        policy.required_scopes = raw.required_scopes.map((scope, item) => text(scope, `${where}.required_scopes[${item}]`));
    }
    if (policy.effect && policy.effect !== "allow" && policy.effect !== "deny") {
        throw new Error(`udbtest: policy "${policy.id ?? ""}" has invalid effect`);
    }
    return policy;
}
/** Parse a policy document: a list of policies, or a mapping with `policies` and an
 *  optional `tenant` that a policy without its own inherits. */
function parsePolicies(source) {
    // JSON is YAML, so one parser reads both. Merge keys match the broker's decoder.
    const documents = (0, yaml_1.parseAllDocuments)(source, { merge: true });
    if (!Array.isArray(documents))
        throw new Error("udbtest: empty policy document");
    if (documents.length > 1)
        throw new Error("udbtest: policy file must contain one document");
    const document = documents[0];
    if (!document)
        throw new Error("udbtest: empty policy document");
    if (document.errors.length > 0)
        throw new Error(`udbtest: parse policy file: ${document.errors[0].message}`);
    const root = document.toJS();
    if (root === null || root === undefined)
        throw new Error("udbtest: empty policy document");
    if (Array.isArray(root))
        return root.map(readPolicy);
    if (!isMap(root))
        throw new Error("udbtest: expected a policy list or policies document");
    if (root.policies === null || root.policies === undefined)
        throw new Error("udbtest: policy document requires policies");
    if (!Array.isArray(root.policies))
        throw schemaError("policies must be a list");
    const tenant = text(root.tenant, "tenant");
    return root.policies.map(readPolicy).map((policy) => ((policy.tenant ?? "").trim() ? policy : { ...policy, tenant }));
}
/** Read a JSON or YAML policy file. */
function loadPolicies(file) {
    let source;
    try {
        source = node_fs_1.default.readFileSync(file, "utf8");
    }
    catch (err) {
        throw new Error(`udbtest: policy file: ${err.message}`);
    }
    return parsePolicies(source);
}
