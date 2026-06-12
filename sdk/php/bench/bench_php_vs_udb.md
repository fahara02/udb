# UDB vs direct PHP — feature-matched micro-benchmark

Iterations: 500 (warm-up 50 discarded) per op, per path. Lower is better.

**Setup (each path has its OWN dockerised backends — no shared cache/contention):**
- baseline: PHP → `pdo_pgsql`/`phpredis` → dedicated Postgres `:55442` + Redis `:56480` (no UDB).
- UDB: PHP → gRPC → dedicated bench broker `:50071` → dedicated Postgres `:55443` + Redis `:56481`.

**Feature-matched:** the baseline replicates UDB's data-plane work — RLS tenant filter on reads, record upsert + transactional outbox insert in one tx on writes, tenant-prefixed cache key. NOT replicated (UDB-only, so UDB does strictly MORE): gRPC encode/decode, Casbin authz, method-security.

| Operation | direct p50 | direct p99 | direct mean | UDB p50 | UDB p99 | UDB mean | UDB÷direct (mean) | UDB−direct (mean) |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| point_read | 9.115 | 33.407 | 10.630 | 21.573 | 806.309 | 48.763 | 4.59× | +38.132 ms |
| point_write | 38.327 | 241.917 | 58.934 | 138.481 | 1490.873 | 227.698 | 3.86× | +168.764 ms |
| fanout_read_20 | 5.305 | 12.376 | 5.643 | 47.621 | 629.274 | 93.387 | 16.55× | +87.744 ms |

All times in **milliseconds**. `UDB−direct (mean)` is the absolute per-call overhead UDB adds (the gRPC hop + broker work the baseline does not do). For point ops this is the cost of the broker indirection; the broker buys pooling, multi-backend uniformity, RLS, authz and the outbox in return.
