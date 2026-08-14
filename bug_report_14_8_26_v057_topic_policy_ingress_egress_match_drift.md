# UDB v0.5.7 topic policy matches differently at enqueue and Kafka publish

Date: 2026-08-14
Status: correction implemented; Kafka publication matrix pending
Affected path: `EnqueueOutboxEvent` topic admission and CDC publisher policy enforcement

## Summary

Ingress treats topic policy `topic` as a WildMatch pattern and accepts
`owning_project="*"`. The CDC engine resolves the same policy by exact topic
equality and treats every non-empty project—including `"*"`—as an exact required
project. Events can be accepted into the outbox and deterministically routed to
DLQ by the publisher under the same policy table.

## Confirmed served path

- `topic_policy_allows` uses `WildMatch::new(&pattern).matches(topic)` and accepts
  project when policy value is empty, `*`, or an exact match.
- `CdcEngine::topic_policy_for` uses `p.topic == topic` only.
- Publisher project enforcement rejects when the policy project is non-empty and
  unequal; it has no `*` exception.
- The policy DDL and runtime type expose a single field, with no separate exact/
  pattern mode to explain the divergent interpretation.

## Consequences

- A row such as `billing.*.v1` admits `billing.invoice.v1` at RPC time, then the
  publisher declares it absent from the active allowlist and DLQs it.
- A wildcard-project policy admits every project at ingress but rejects every
  non-literal-`*` envelope at egress.
- Clients receive `enqueued=true` for work that can never reach its Kafka topic.

## Required correction

- Define one canonical compiled matcher and scope predicate shared by ingress,
  publisher, subscription, and tests.
- Validate policy rows at write/provision time and expose exact vs pattern
  semantics explicitly if both are required.
- Add real Kafka tests for exact, wildcard topic, empty/wildcard/exact project,
  rejection, and successful publication.

## Verification log

- Source trace completed across live topic-policy query, cached engine lookup,
  publisher tenant/project checks, and policy DDL.
- Ingress and the CDC snapshot now share the same canonical WildMatch topic and
  empty/`*`/exact tenant/project predicates; the publisher no longer interprets
  topic or project wildcards as literal exact values.
- Focused wildcard/scope and disabled-final-policy tests passed, and the
  Kafka-enabled library compiles. The real Kafka exact/wildcard matrix remains
  delegated to CI.
