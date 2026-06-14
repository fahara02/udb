<?php

declare(strict_types=1);

// =============================================================================
// UDB vs direct-PHP, feature-matched micro-benchmark.
//
// Two paths execute the SAME logical operation, each against its OWN dedicated
// dockerised backends (so neither path warms or contends the other's cache):
//
//   * BASELINE (no UDB): PHP -> native driver -> dedicated Postgres/Redis.
//       - pdo_pgsql to bench Postgres (BENCH_PG_*)
//       - phpredis  to bench Redis    (BENCH_REDIS_*)
//   * UDB:               PHP -> gRPC -> dedicated bench broker -> its own Postgres/Redis.
//
// FEATURE-MATCHED: the baseline replicates UDB's data-plane work so we isolate the
// transport/broker overhead, not "UDB does more":
//   - reads:  filtered by tenant_id (manual RLS).
//   - writes: record upsert AND a transactional outbox insert, in ONE transaction
//             (mirrors UDB's transactional outbox / CDC event).
//   - cache:  tenant-prefixed key.
// NOT replicated (UDB-only, so UDB still does STRICTLY MORE than the baseline):
//   gRPC encode/decode, Casbin authz, the method-security tower layer. Any UDB
//   overhead beyond the measured gap is buying those guarantees.
// =============================================================================

require __DIR__ . '/../vendor/autoload.php';

use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\UdbAuthClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Udb\Core\Authn\Services\V1\LoginRequest;

function envv(string $k, string $d): string
{
    $v = getenv($k);
    return ($v === false || $v === '') ? $d : $v;
}

function meta(string $tenant, string $project, string $bearer = ''): UdbMetadata
{
    return new UdbMetadata(
        tenantId: $tenant,
        userId: '',
        purpose: 'php.bench',
        correlationId: 'php-bench',
        scopes: [],
        serviceIdentity: 'php.bench',
        projectId: $project,
        clientCatalogVersion: '1.0.0',
        bearerToken: $bearer,
    );
}

function liveStruct(array $fields): \Google\Protobuf\Struct
{
    $struct = new \Google\Protobuf\Struct();
    $map = $struct->getFields();
    foreach ($fields as $key => $value) {
        $v = new \Google\Protobuf\Value();
        if (is_bool($value)) {
            $v->setBoolValue($value);
        } elseif (is_int($value) || is_float($value)) {
            $v->setNumberValue($value);
        } else {
            $v->setStringValue((string) $value);
        }
        $map[$key] = $v;
    }
    return $struct;
}

function recordJson(string $recordId, string $tenant, string $project, string $lk, string $payload, int $rev): string
{
    return json_encode([
        'record_id' => $recordId, 'tenant_id' => $tenant, 'project_id' => $project,
        'lookup_key' => $lk, 'payload' => $payload, 'revision' => $rev,
    ]);
}

/** Time $fn over $iters iterations after $warmup discarded warm-ups. Returns ms stats. */
function bench(callable $fn, int $iters, int $warmup): array
{
    for ($i = 0; $i < $warmup; $i++) {
        $fn($i);
    }
    $durs = [];
    for ($i = 0; $i < $iters; $i++) {
        $t = hrtime(true);
        $fn($i);
        $durs[] = (hrtime(true) - $t) / 1e6; // ns -> ms
    }
    sort($durs);
    $pct = fn (int $p) => $durs[min(count($durs) - 1, intdiv($p * (count($durs) - 1), 100))];
    return ['p50' => $pct(50), 'p99' => $pct(99), 'mean' => array_sum($durs) / count($durs)];
}

// ---- params ----
$ITERS  = (int) envv('BENCH_ITERS', '500');
$WARMUP = (int) envv('BENCH_WARMUP', '50');
$FANOUT = (int) envv('BENCH_FANOUT', '20');
$messageType = 'udb.sdk.live.v1.SdkLiveRecord';
$project = envv('UDB_LIVE_PROJECT', 'default');

// ---- baseline: dedicated Postgres + Redis ----
$pdo = new PDO(
    sprintf('pgsql:host=%s;port=%s;dbname=udb', envv('BENCH_PG_HOST', 'host.docker.internal'), envv('BENCH_PG_PORT', '55442')),
    'udb', 'udb',
    [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_EMULATE_PREPARES => false]
);
$redis = new Redis();
$redis->connect(envv('BENCH_REDIS_HOST', 'host.docker.internal'), (int) envv('BENCH_REDIS_PORT', '56480'));

$pdo->exec('CREATE TABLE IF NOT EXISTS bench_records (
  tenant_id text NOT NULL, project_id text NOT NULL, record_id text NOT NULL,
  lookup_key text, payload text, revision int, created_at timestamptz DEFAULT now(),
  PRIMARY KEY (tenant_id, record_id))');
$pdo->exec('CREATE TABLE IF NOT EXISTS bench_outbox (
  event_seq bigserial PRIMARY KEY, event_id uuid NOT NULL, topic text NOT NULL,
  partition_key text, payload jsonb, created_at timestamptz DEFAULT now())');

// ---- UDB: dedicated bench broker ----
$target = envv('UDB_GRPC_TARGET', 'host.docker.internal:50071');
$authTarget = envv('UDB_AUTH_GRPC_TARGET', 'host.docker.internal:50081');
$bootMeta = meta(envv('UDB_LIVE_TENANT', 'sdk-live'), $project);
$openAuth = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 10000, 'retry' => ['max_attempts' => 1]]);
$openAuth->bindContext($bootMeta);
$login = $openAuth->login(
    (new LoginRequest())
        ->setUsername(envv('UDB_LIVE_USERNAME', 'sdk-live-admin'))
        ->setPassword(envv('UDB_LIVE_PASSWORD', ''))
        ->setTenantHint($bootMeta->tenantId)
        ->setProjectHint($project)
        ->setDeviceName('php-bench'),
    $bootMeta
);
$auth = new UdbAuthClient(['endpoint' => $authTarget, 'deadline_ms' => 10000]);
$auth->bindContext($bootMeta);
$who = $auth->authenticateBearer($login->getAccessToken(), $bootMeta);
$tenant = $who?->getPrincipal()?->getTenantId() ?: $bootMeta->tenantId; // canonical UUID
$authedMeta = meta($tenant, $project, $login->getAccessToken());
$data = new GeneratedClient(['endpoint' => $target, 'deadline_ms' => 15000, 'retry' => ['max_attempts' => 1]]);
$data->bindContext($authedMeta);
$ctx = (new \Udb\Entity\V1\RequestContext())->setTenantId($tenant)->setProjectId($project)->setPurpose('php.bench');

fwrite(STDERR, "tenant=$tenant  iters=$ITERS warmup=$WARMUP fanout=$FANOUT\n");

// ---- seed identical data on BOTH paths (same tenant UUID, same row count) ----
// Seed a realistic row population so the list/aggregate ops scan real data.
$SEED = (int) envv('BENCH_SEED', '200');
$ins = $pdo->prepare('INSERT INTO bench_records (tenant_id,project_id,record_id,lookup_key,payload,revision)
  VALUES (:t,:p,:r,:lk,:pl,1) ON CONFLICT (tenant_id,record_id) DO UPDATE SET payload=EXCLUDED.payload');
for ($i = 0; $i < $SEED; $i++) {
    $rid = "bench-seed-$i";
    $ins->execute([':t' => $tenant, ':p' => $project, ':r' => $rid, ':lk' => "lk-$i", ':pl' => "seed-$i"]);
    $data->upsert((new \Udb\Entity\V1\UpsertRequest())->setContext($ctx)->setMessageType($messageType)
        ->setRecordJson(recordJson($rid, $tenant, $project, "lk-$i", "seed-$i", 1))
        ->setConflictFields(['record_id']), $authedMeta);
}
$readId = 'bench-seed-0';

// ---- ops ----
$selRead = $pdo->prepare('SELECT payload FROM bench_records WHERE tenant_id=:t AND record_id=:r LIMIT 1');
$selFan  = $pdo->prepare('SELECT payload FROM bench_records WHERE tenant_id=:t LIMIT :n');
$wrUp    = $pdo->prepare('INSERT INTO bench_records (tenant_id,project_id,record_id,lookup_key,payload,revision)
  VALUES (:t,:p,:r,:lk,:pl,:rev) ON CONFLICT (tenant_id,record_id) DO UPDATE SET payload=EXCLUDED.payload, revision=EXCLUDED.revision');
$wrOut   = $pdo->prepare('INSERT INTO bench_outbox (event_id,topic,partition_key,payload)
  VALUES (gen_random_uuid(), :topic, :pk, :payload)');
$selList50  = $pdo->prepare('SELECT record_id,payload,revision FROM bench_records WHERE tenant_id=:t LIMIT 50');
$selList200 = $pdo->prepare('SELECT record_id,payload,revision FROM bench_records WHERE tenant_id=:t LIMIT 200');
$wrRet      = $pdo->prepare('INSERT INTO bench_records (tenant_id,project_id,record_id,lookup_key,payload,revision)
  VALUES (:t,:p,:r,:lk,:pl,1) ON CONFLICT (tenant_id,record_id) DO UPDATE SET payload=EXCLUDED.payload
  RETURNING record_id,payload,revision');

$ops = [
    'point_read' => [
        // feature-matched: RLS tenant filter on a single keyed row.
        'direct' => function () use ($selRead, $tenant, $readId) {
            $selRead->execute([':t' => $tenant, ':r' => $readId]);
            $selRead->fetch();
        },
        'udb' => function () use ($data, $ctx, $authedMeta, $messageType, $tenant, $project, $readId) {
            $data->select((new \Udb\Entity\V1\SelectRequest())->setContext($ctx)->setMessageType($messageType)
                ->setFilter(liveStruct(['record_id' => $readId, 'tenant_id' => $tenant, 'project_id' => $project]))
                ->setLimit(1), $authedMeta);
        },
    ],
    'point_write' => [
        // feature-matched: record upsert + transactional outbox insert, one tx.
        'direct' => function (int $i) use ($pdo, $wrUp, $wrOut, $tenant, $project) {
            $rid = "bench-w-$i";
            $pdo->beginTransaction();
            $wrUp->execute([':t' => $tenant, ':p' => $project, ':r' => $rid, ':lk' => "wlk-$i", ':pl' => "w-$i", ':rev' => 1]);
            $wrOut->execute([':topic' => 'udb.sdk.live.v1.SdkLiveRecord', ':pk' => $tenant, ':payload' => json_encode(['record_id' => $rid, 'payload' => "w-$i"])]);
            $pdo->commit();
        },
        'udb' => function (int $i) use ($data, $ctx, $authedMeta, $messageType, $tenant, $project) {
            $rid = "bench-w-$i";
            $data->upsert((new \Udb\Entity\V1\UpsertRequest())->setContext($ctx)->setMessageType($messageType)
                ->setRecordJson(recordJson($rid, $tenant, $project, "wlk-$i", "w-$i", 1))
                ->setConflictFields(['record_id']), $authedMeta);
        },
    ],
    'cache_set_get' => [
        // feature-matched: tenant-prefixed key, set+get.
        'direct' => function (int $i) use ($redis, $tenant) {
            $k = "udb:$tenant:bench:" . ($i % 64);
            $redis->setex($k, 60, "v-$i");
            $redis->get($k);
        },
        'udb' => function (int $i) use ($data, $ctx, $authedMeta, $tenant) {
            $res = (new \Udb\Entity\V1\StoreResource())->setBackend('redis');
            $k = 'bench:' . ($i % 64);
            $data->cache_set((new \Udb\Entity\V1\CacheSetRequest())->setContext($ctx)->setResource($res)
                ->setKey($k)->setValue("v-$i")->setContentType('text/plain')->setTtlSeconds(60), $authedMeta);
            $data->cache_get((new \Udb\Entity\V1\CacheGetRequest())->setContext($ctx)->setResource($res)->setKey($k), $authedMeta);
        },
    ],
    'fanout_read_' . $FANOUT => [
        // feature-matched: N rows of the tenant in ONE call (both batched).
        'direct' => function () use ($pdo, $tenant, $FANOUT) {
            $st = $pdo->prepare('SELECT payload FROM bench_records WHERE tenant_id=:t LIMIT :n');
            $st->bindValue(':t', $tenant);
            $st->bindValue(':n', $FANOUT, PDO::PARAM_INT);
            $st->execute();
            $st->fetchAll();
        },
        'udb' => function () use ($data, $ctx, $authedMeta, $messageType, $tenant, $project, $FANOUT) {
            $data->select((new \Udb\Entity\V1\SelectRequest())->setContext($ctx)->setMessageType($messageType)
                ->setFilter(liveStruct(['tenant_id' => $tenant, 'project_id' => $project]))->setLimit($FANOUT), $authedMeta);
        },
    ],
    // ---- HARDER / real-world ops (amortise the fixed per-call overhead) ----
    'list_read_50' => [
        // realistic "list endpoint": return 50 tenant rows in one call.
        'direct' => function () use ($selList50, $tenant) {
            $selList50->execute([':t' => $tenant]);
            $selList50->fetchAll();
        },
        'udb' => function () use ($data, $ctx, $authedMeta, $messageType, $tenant, $project) {
            $data->select((new \Udb\Entity\V1\SelectRequest())->setContext($ctx)->setMessageType($messageType)
                ->setFilter(liveStruct(['tenant_id' => $tenant, 'project_id' => $project]))->setLimit(50), $authedMeta);
        },
    ],
    'list_read_200' => [
        // heavier list: 200 rows.
        'direct' => function () use ($selList200, $tenant) {
            $selList200->execute([':t' => $tenant]);
            $selList200->fetchAll();
        },
        'udb' => function () use ($data, $ctx, $authedMeta, $messageType, $tenant, $project) {
            $data->select((new \Udb\Entity\V1\SelectRequest())->setContext($ctx)->setMessageType($messageType)
                ->setFilter(liveStruct(['tenant_id' => $tenant, 'project_id' => $project]))->setLimit(200), $authedMeta);
        },
    ],
    'write_read_roundtrip' => [
        // realistic "create-and-return": write a record + outbox, return the row.
        'direct' => function (int $i) use ($pdo, $wrRet, $wrOut, $tenant, $project) {
            $rid = "bench-wr-$i";
            $pdo->beginTransaction();
            $wrRet->execute([':t' => $tenant, ':p' => $project, ':r' => $rid, ':lk' => "wrlk-$i", ':pl' => "wr-$i"]);
            $wrRet->fetch();
            $wrOut->execute([':topic' => 'udb.sdk.live.v1.SdkLiveRecord', ':pk' => $tenant, ':payload' => json_encode(['record_id' => $rid])]);
            $pdo->commit();
        },
        'udb' => function (int $i) use ($data, $ctx, $authedMeta, $messageType, $tenant, $project) {
            $rid = "bench-wr-$i";
            $data->upsert((new \Udb\Entity\V1\UpsertRequest())->setContext($ctx)->setMessageType($messageType)
                ->setRecordJson(recordJson($rid, $tenant, $project, "wrlk-$i", "wr-$i", 1))
                ->setConflictFields(['record_id'])->setReturnRecord(true), $authedMeta);
        },
    ],
];

// The cache op needs a Redis backend on the broker; skip it when running against a
// Postgres-only broker (e.g. a lint build without the redis Cargo feature).
if (envv('BENCH_SKIP_CACHE', '0') === '1') {
    unset($ops['cache_set_get']);
}

// ---- run ----
$rows = [];
foreach ($ops as $name => $impl) {
    $d = bench($impl['direct'], $ITERS, $WARMUP);
    $u = bench($impl['udb'], $ITERS, $WARMUP);
    $rows[] = ['op' => $name, 'd' => $d, 'u' => $u];
    fwrite(STDERR, sprintf("  %-18s direct p50=%.3fms  udb p50=%.3fms\n", $name, $d['p50'], $u['p50']));
}

// ---- report ----
$L = [];
$L[] = '# UDB vs direct PHP — feature-matched micro-benchmark';
$L[] = '';
$L[] = sprintf('Iterations: %d (warm-up %d discarded) per op, per path. Lower is better.', $ITERS, $WARMUP);
$L[] = '';
$L[] = '**Setup (each path has its OWN dockerised backends — no shared cache/contention):**';
$L[] = '- baseline: PHP → `pdo_pgsql`/`phpredis` → dedicated Postgres `:55442` + Redis `:56480` (no UDB).';
$L[] = '- UDB: PHP → gRPC → dedicated bench broker `:50071` → dedicated Postgres `:55443` + Redis `:56481`.';
$L[] = '';
$L[] = '**Feature-matched:** the baseline replicates UDB\'s data-plane work — RLS tenant filter on reads, '
    . 'record upsert + transactional outbox insert in one tx on writes, tenant-prefixed cache key. '
    . 'NOT replicated (UDB-only, so UDB does strictly MORE): gRPC encode/decode, Casbin authz, method-security.';
$L[] = '';
$L[] = '| Operation | direct p50 | direct p99 | direct mean | UDB p50 | UDB p99 | UDB mean | UDB÷direct (mean) | UDB−direct (mean) |';
$L[] = '|---|--:|--:|--:|--:|--:|--:|--:|--:|';
foreach ($rows as $r) {
    $L[] = sprintf(
        '| %s | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f | %.2f× | +%.3f ms |',
        $r['op'], $r['d']['p50'], $r['d']['p99'], $r['d']['mean'],
        $r['u']['p50'], $r['u']['p99'], $r['u']['mean'],
        $r['u']['mean'] / max($r['d']['mean'], 0.0001),
        $r['u']['mean'] - $r['d']['mean']
    );
}
$L[] = '';
$L[] = 'All times in **milliseconds**. `UDB−direct (mean)` is the absolute per-call overhead UDB adds '
    . '(the gRPC hop + broker work the baseline does not do). For point ops this is the cost of the broker '
    . 'indirection; the broker buys pooling, multi-backend uniformity, RLS, authz and the outbox in return.';
file_put_contents(__DIR__ . '/bench_php_vs_udb.md', implode("\n", $L) . "\n");
fwrite(STDERR, "wrote bench/bench_php_vs_udb.md\n");
echo implode("\n", $L) . "\n";
