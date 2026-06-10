#!/usr/bin/env node
// Fast repository gate for enterprise-readiness artifacts.
// This does not replace live tests; it prevents release branches from dropping
// the CI hooks, runbooks, load harnesses, or conformance runner that make those
// live checks repeatable.

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const repo = process.cwd();
const requiredFiles = [
  "sdk-conformance/run.mjs",
  "docs/operations.md",
  "scripts/native-load-test.sh",
  "scripts/native-load-test.ps1",
];

const requiredCiSnippets = [
  "name: Python SDK (pytest)",
  "name: SDK conformance (all languages)",
  "node sdk-conformance/run.mjs typescript python go csharp java php",
  "node scripts/check-enterprise-readiness.mjs",
];

const requiredRunbookTerms = [
  "CDC backlog",
  "DLQ recovery",
  "native service dependency outage",
  "object/vector backend partial failure",
  "leader-election failover",
  "bad policy rollout rollback",
  "access review workflow",
];

const requiredCodeEvidence = [
  {
    file: "src/runtime/service/native_helpers.rs",
    snippets: [
      "validate_native_compliance(",
      "enterprise_audit_mode()",
      "crate::runtime::otel::current_actor()",
      "inc_outbox_enqueue_failures_total(\"native_compliance\")",
    ],
  },
  {
    file: "src/runtime/service/method_security.rs",
    snippets: ["scope_principal(principal, fut)", "MethodSecurityService"],
  },
  {
    file: "src/runtime/service/native_registry.rs",
    snippets: [
      "health_statuses_are_coherent_with_mounted_surface",
      "registry_mounted_proto_services_match_native_descriptor_surface",
    ],
  },
  {
    file: "src/runtime/config/tests.rs",
    snippets: [
      "production_service_settings_default_to_secure_internal_transport",
      "compliance_hardened_yaml_matches_secure_runtime_posture",
    ],
  },
  {
    file: "src/runtime/cdc/engine_tail.rs",
    snippets: [
      "try_mark_cdc_delivery_state",
      "journal insert failed",
      "was_durably_published",
    ],
  },
  {
    file: "src/proto_redaction.rs",
    snippets: ["Descriptor-driven outbound DTO redaction", "dto_redaction.rs"],
  },
  {
    file: "src/runtime/authn/profile.rs",
    snippets: ["redact_profile_attributes_json", "profile_redaction_scrubs_sensitive_nested_keys"],
  },
  {
    file: "src/runtime/authn/revocation.rs",
    snippets: ["revocation_lookup_error_outcome", "REVOCATION_STORE_UNAVAILABLE_REASON"],
  },
  {
    file: "src/runtime/service/auth_service/mappings.rs",
    snippets: ["RedactStorageOnly::redact_storage_only"],
  },
  {
    file: "src/runtime/service/auth_service/authn/core.rs",
    snippets: ["RedactStorageOnly::redact_storage_only"],
  },
  {
    file: "src/runtime/service/auth_service/audit_export.rs",
    snippets: ["actor", "target_resource", "operation", "outcome"],
  },
  {
    file: "src/cli/output.rs",
    snippets: ["descriptor_sensitive_output_keys", "redact_sensitive_json"],
  },
];

const failures = [];

for (const file of requiredFiles) {
  if (!existsSync(join(repo, file))) {
    failures.push(`missing required artifact: ${file}`);
  }
}

const ciPath = join(repo, ".github", "workflows", "ci.yml");
const ci = existsSync(ciPath) ? readFileSync(ciPath, "utf8") : "";
for (const snippet of requiredCiSnippets) {
  if (!ci.includes(snippet)) {
    failures.push(`ci.yml missing gate: ${snippet}`);
  }
}

const operationsPath = join(repo, "docs", "operations.md");
const operations = existsSync(operationsPath) ? readFileSync(operationsPath, "utf8") : "";
for (const term of requiredRunbookTerms) {
  if (!operations.toLowerCase().includes(term.toLowerCase())) {
    failures.push(`operations doc missing runbook section/term: ${term}`);
  }
}

for (const term of ["sdk-conformance/run.mjs", "native-load-test", "multi-node", "compliance"]) {
  if (!operations.toLowerCase().includes(term.toLowerCase())) {
    failures.push(`operations doc missing readiness evidence term: ${term}`);
  }
}

for (const check of requiredCodeEvidence) {
  const path = join(repo, check.file);
  if (!existsSync(path)) {
    failures.push(`missing enterprise code evidence file: ${check.file}`);
    continue;
  }
  const text = readFileSync(path, "utf8");
  for (const snippet of check.snippets) {
    if (!text.includes(snippet)) {
      failures.push(`enterprise code evidence missing in ${check.file}: ${snippet}`);
    }
  }
}

if (failures.length) {
  console.error("Enterprise-readiness gate failed:");
  for (const failure of failures) console.error(`- ${failure}`);
  process.exit(1);
}

console.log("Enterprise-readiness artifacts are wired.");
