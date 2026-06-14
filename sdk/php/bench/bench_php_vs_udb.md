# UDB vs direct PHP — feature-matched micro-benchmark

Iterations: 300 (warm-up 40 discarded) per op, per path. Lower is better.

**Setup (each path has its OWN dockerised backends — no shared cache/contention):**
- baseline: PHP → `pdo_pgsql`/`phpredis` → dedicated Postgres `:55442` + Redis `:56480` (no UDB).
- UDB: PHP → gRPC → dedicated bench broker `:50071` → dedicated Postgres `:55443` + Redis `:56481`.

**Feature-matched:** the baseline replicates UDB's data-plane work — RLS tenant filter on reads, record upsert + transactional outbox insert in one tx on writes, tenant-prefixed cache key. NOT replicated (UDB-only, so UDB does strictly MORE): gRPC encode/decode, Casbin authz, method-security.

| Operation | direct p50 | direct p99 | direct mean | UDB p50 | UDB p99 | UDB mean | UDB÷direct (mean) | UDB−direct (mean) |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| point_read | 1.002 | 1.801 | 1.052 | 3.945 | 8.627 | 4.342 | 4.13× | +3.290 ms |
| point_write | 5.614 | 9.817 | 5.648 | 13.270 | 21.671 | 13.978 | 2.48× | +8.331 ms |
| cache_set_get | 1.982 | 4.080 | 2.035 | 7.219 | 24.472 | 7.930 | 3.90× | +5.895 ms |
| fanout_read_20 | 2.834 | 5.737 | 3.017 | 6.079 | 10.294 | 6.464 | 2.14× | +3.447 ms |
| list_read_50 | 1.367 | 2.601 | 1.417 | 6.523 | 13.325 | 6.849 | 4.83× | +5.432 ms |
| list_read_200 | 1.381 | 4.075 | 1.562 | 6.306 | 14.810 | 6.846 | 4.38× | +5.284 ms |
| write_read_roundtrip | 4.706 | 8.540 | 4.957 | 14.430 | 26.499 | 15.266 | 3.08× | +10.309 ms |

All times in **milliseconds**. `UDB−direct (mean)` is the absolute per-call overhead UDB adds (the gRPC hop + broker work the baseline does not do). For point ops this is the cost of the broker indirection; the broker buys pooling, multi-backend uniformity, RLS, authz and the outbox in return.
