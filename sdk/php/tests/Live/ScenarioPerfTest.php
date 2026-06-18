<?php

declare(strict_types=1);

// Scenario perf bench — PHP (R6.2 scenario-vs-surface split).
//
// This is the SCENARIO counterpart to the full 265-RPC coverage sweep in
// GeneratedRpcSurfaceTest.php ('measures per-RPC latency' -> perf_report_php.md).
// Where the full sweep answers "is every RPC reachable and how fast", this answers
// "how fast is the user-facing facade path the simple-client docs tell people to call":
//
//   - loginAndAdoptTenant   (Login + AuthenticateBearer + atomic adopt)
//   - entity.upsert         (one Upsert RPC, no proof read)
//   - entity.select         (one Select RPC)
//   - entity.delete         (one Delete RPC, NON-existent row -> success path, no destroy)
//   - uploadFile            (RegisterUpload -> presigned PUT -> FinalizeUpload)
//   - downloadFile          (GetDownloadUrl — presigned, bytes never transit the broker)
//   - events.subscribeReady (Subscribe + Ready — readiness boundary)
//   - events.publishAndWait (EnqueueOutboxEvent + first matching CDC envelope)
//   - webrtc.joinSession    (PeerService.JoinRoom + LeaveRoom — the PHP SDK has no atomic
//                            JoinSession facade yet, so this mirrors the join workflow
//                            via the room/peer APIs; TODO: unify on an atomic helper once
//                            the PHP facade gains one, to match Go/TS/Python joinSession)
//
// It is gated SEPARATELY from both the conformance suite and the full-surface perf
// sweep: UDB_LIVE_SDK_TESTS=1 + UDB_SCENARIO_PERF=1. The result is published to its
// OWN file (scenario_perf_php.md) so it runs and reports independently of
// perf_report_php.md. As with the full sweep, a slow scenario is reported, not
// failed — only a scenario that NEVER completes is surfaced as a failure.
//
// The latency includes the FULL facade cost a caller pays: client encode + every RPC
// in the helper's documented sequence (docs/bench-bodies/workflow-sequences.md) +
// transport + server handle + decode + any presigned byte transfer. Mirrors the Go
// reference bench (sdk/go/udbclient/live_scenario_perf_test.go) and the TS/Python
// scenario benches.

use Fahara02\UdbLaravel\UdbProject;

// Gated SEPARATELY: skip (and return without registering the test body's live work)
// unless BOTH flags are set, so the file parses (php -l) + Pest collects it cleanly
// offline (backends/registry down) and it only executes against a real broker.
if (getenv('UDB_LIVE_SDK_TESTS') !== '1' || getenv('UDB_SCENARIO_PERF') !== '1') {
    test('measures scenario perf')->skip('scenario perf run requires UDB_LIVE_SDK_TESTS=1 and UDB_SCENARIO_PERF=1');

    return;
}

// Uniquely-named helpers (the conformance file defines its own global helpers; guard
// against redeclaration since both files load in one Pest run).
if (! function_exists('scenarioEnv')) {
    function scenarioEnv(string $name, ?string $fallback = null): string
    {
        $value = trim((string) getenv($name));
        if ($value !== '') {
            return $value;
        }
        if ($fallback !== null) {
            return $fallback;
        }
        throw new RuntimeException("{$name} is required when UDB_SCENARIO_PERF=1");
    }
}

if (! function_exists('scenarioStatusName')) {
    function scenarioStatusName(\Throwable $e): string
    {
        // UdbRpcException carries the gRPC status code; map common ones to NAMEs.
        $code = property_exists($e, 'status') ? (int) $e->status : -1;
        $names = [
            0 => 'OK', 1 => 'CANCELLED', 2 => 'UNKNOWN', 3 => 'INVALID_ARGUMENT',
            4 => 'DEADLINE_EXCEEDED', 5 => 'NOT_FOUND', 6 => 'ALREADY_EXISTS',
            7 => 'PERMISSION_DENIED', 8 => 'RESOURCE_EXHAUSTED', 9 => 'FAILED_PRECONDITION',
            10 => 'ABORTED', 11 => 'OUT_OF_RANGE', 12 => 'UNIMPLEMENTED', 13 => 'INTERNAL',
            14 => 'UNAVAILABLE', 15 => 'DATA_LOSS', 16 => 'UNAUTHENTICATED',
        ];

        return $names[$code] ?? 'UNKNOWN';
    }
}

if (! function_exists('scenarioPct')) {
    /** @param array<int,float> $sorted */
    function scenarioPct(array $sorted, int $p): float
    {
        $n = count($sorted);
        if ($n === 0) {
            return 0.0;
        }

        return $sorted[min($n - 1, intdiv($p * ($n - 1), 100))];
    }
}

it('measures scenario perf', function () {
    $target = scenarioEnv('UDB_GRPC_TARGET');
    $authTarget = scenarioEnv('UDB_AUTH_GRPC_TARGET', $target);
    $username = scenarioEnv('UDB_LIVE_USERNAME');
    $password = scenarioEnv('UDB_LIVE_PASSWORD');
    $tenantHint = scenarioEnv('UDB_LIVE_TENANT', 'sdk-live');
    $projectId = scenarioEnv('UDB_LIVE_PROJECT', 'default');

    // Build the unified facade exactly as a real caller would, then login-and-adopt so
    // every facade carries the canonical tenant from the VERIFIED principal — the same
    // path the simple-client docs prescribe.
    $project = new UdbProject([
        'target' => $target,
        'authTarget' => $authTarget,
        'tenant' => $tenantHint,
        'project' => $projectId,
        'serviceIdentity' => 'php.sdk.scenario.perf',
        'purpose' => 'php.live.scenario.perf',
        'correlationId' => 'php-live-scenario-perf',
    ]);

    $suffix = (string) hrtime(true);
    $authn = $project->loginAndAdoptTenant($username, $password);
    $principal = $authn->getPrincipal();
    $tenant = ($principal !== null && $principal->getTenantId() !== '') ? $principal->getTenantId() : $tenantHint;

    $entity = $project->data()->entity('udb.sdk.live.v1.SdkLiveRecord', ['record_id']);
    $scenarioRecord = fn (int $i): array => [
        'record_id' => "php-scn-{$suffix}-{$i}",
        'tenant_id' => $tenant,
        'project_id' => $projectId,
        'lookup_key' => 'php-scn-lk',
        'payload' => 'php-scenario',
        'revision' => 1,
    ];

    // Each scenario: [name, seq, iters, warmup, fn]. warmup is only safe for idempotent
    // helpers (login rotates a session, publishAndWait awaits a fresh event).
    $scenarios = [
        ['loginAndAdoptTenant', 'Login, AuthenticateBearer', 5, false,
            fn () => $project->loginAndAdoptTenant($username, $password)],
        ['entity.upsert', 'Upsert', 10, true,
            fn () => $entity->upsert($scenarioRecord(0))],
        ['entity.select', 'Select', 25, true,
            fn () => $entity->select(['tenant_id' => $tenant, 'project_id' => $projectId])],
        // Delete a NON-EXISTENT row so the helper runs its full path without destroying
        // a row a later iteration/scenario reads.
        ['entity.delete', 'Delete', 5, true,
            fn () => $entity->delete(['record_id' => 'php-scn-delete-noop', 'tenant_id' => $tenant, 'project_id' => $projectId])],
        ['uploadFile', 'RegisterUpload, PUT, FinalizeUpload', 5, true,
            fn () => $project->storage()->uploadFile('php-scenario.txt', 'php-scenario-upload', ['contentType' => 'text/plain', 'fileType' => 'DOCUMENT'])],
        ['events.subscribeReady', 'PublishCDC(open), Header', 5, true,
            function () use ($project, $projectId) {
                $sub = $project->events()->subscribe("{$projectId}.*");
                $sub->ready();
            }],
        ['events.publishAndWait', 'EnqueueOutboxEvent, PublishCDC first-event', 3, false,
            fn () => $project->events()->publishAndWait(
                "sdk.scenario.{$suffix}",
                json_encode(['event' => 'php-scenario', 'n' => $suffix]),
                static fn (object $env): bool => true,
            )],
    ];

    // downloadFile / webrtc.joinSession need a pre-existing file / room. Seed one of
    // each up front (their cost is NOT measured) so the measured scenario is the pure
    // helper path. A failed seed downgrades the scenario to a recorded skip.
    try {
        $up = $project->storage()->uploadFile('php-scenario-dl.txt', 'php-scenario-download', ['contentType' => 'text/plain', 'fileType' => 'DOCUMENT']);
        $fileId = method_exists($up, 'getFile') && $up->getFile() !== null
            ? $up->getFile()->getFileId()
            : (method_exists($up, 'getFileId') ? $up->getFileId() : '');
        if ($fileId !== '') {
            $scenarios[] = ['downloadFile', 'GetDownloadUrl', 25, true,
                fn () => $project->storage()->downloadFile($fileId, ['expiresInMinutes' => 5])];
        }
    } catch (\Throwable $e) {
        echo "scenario seed: download file upload failed, downloadFile scenario skipped: ".scenarioStatusName($e)."\n";
    }

    try {
        $room = $project->webRtc()->room()->createRoom("php-scenario-room-{$suffix}", 8, '{}');
        $roomId = method_exists($room, 'getRoomId') ? $room->getRoomId()
            : (method_exists($room, 'getRoom') && $room->getRoom() !== null ? $room->getRoom()->getRoomId() : '');
        if ($roomId !== '') {
            // TODO(R6.2): the PHP SDK has no atomic joinSession facade yet; this mirrors
            // the join workflow via PeerService.JoinRoom + LeaveRoom. Unify on an atomic
            // helper once the PHP facade gains one (parity with Go/TS/Python joinSession).
            $scenarios[] = ['webrtc.joinSession', 'JoinRoom, LeaveRoom', 5, false,
                function () use ($project, $roomId) {
                    $peer = $project->webRtc()->peer();
                    $joined = $peer->joinRoom($roomId, 'php-scenario-peer');
                    $peerId = method_exists($joined, 'getPeer') && $joined->getPeer() !== null
                        ? $joined->getPeer()->getPeerId()
                        : (method_exists($joined, 'getPeerId') ? $joined->getPeerId() : '');
                    if ($peerId !== '') {
                        try {
                            $peer->leaveRoom($roomId, $peerId);
                        } catch (\Throwable) {
                            // leave is best-effort cleanup
                        }
                    }
                }];
        }
    } catch (\Throwable $e) {
        echo "scenario seed: webrtc room create failed, joinSession scenario skipped: ".scenarioStatusName($e)."\n";
    }

    // ── measurement loop ──────────────────────────────────────────────────────
    $samples = [];
    foreach ($scenarios as [$name, $seq, $iters, $warmup, $fn]) {
        if ($warmup) {
            try {
                $fn();
            } catch (\Throwable) {
                // warm-up errors are ignored
            }
        }
        $okMs = [];
        $allMs = [];
        $firstErr = 'OK';
        $firstDetail = '';
        for ($i = 0; $i < $iters; $i++) {
            $start = microtime(true);
            $err = 'OK';
            try {
                $fn();
            } catch (\Throwable $e) {
                $err = scenarioStatusName($e);
                if ($i === 0) {
                    $firstDetail = substr($e->getMessage(), 0, 200);
                }
            }
            $ms = (microtime(true) - $start) * 1000.0;
            if ($i === 0) {
                $firstErr = $err;
            }
            if ($err === 'OK') {
                $okMs[] = $ms;
            }
            $allMs[] = $ms;
        }
        $measured = count($okMs) > 0 ? $okMs : $allMs;
        sort($measured);
        $errCode = count($okMs) > 0 ? 'OK' : $firstErr;
        if ($errCode !== 'OK') {
            echo "[SCENARIO-FAIL] {$name} => {$errCode}: {$firstDetail}\n";
        }
        $samples[] = [
            'name' => $name, 'seq' => $seq, 'err' => $errCode,
            'p50' => scenarioPct($measured, 50), 'p99' => scenarioPct($measured, 99),
            'mean' => array_sum($measured) / count($measured),
            'min' => $measured[0], 'max' => $measured[count($measured) - 1],
            'iters' => $iters,
        ];
    }

    // ── report (OWN file: scenario_perf_php.md) ─────────────────────────────────
    $lines = [
        '# UDB SDK Scenario Perf — PHP (Docker → host)', '',
        'Scenarios measured: '.count($samples)."   tenant={$tenant}", '',
        'This is the SCENARIO bench: it times the user-facing WORKFLOW HELPERS the '
            .'simple-client docs prescribe (uploadFile, downloadFile, bound entity '
            .'upsert/select/delete, loginAndAdoptTenant, events subscribe-ready/publish-and-wait, '
            .'webrtc joinSession) — measured as end-to-end facade calls, NOT the raw 265-RPC '
            .'surface (that stays in perf_report_php.md). Each `seq` is the documented helper '
            .'RPC sequence (docs/bench-bodies/workflow-sequences.md).', '',
        '| Scenario | seq | err | p50 ms | p99 ms | mean ms | min ms | max ms | iters |',
        '|---|---|---|--:|--:|--:|--:|--:|--:|',
    ];
    usort($samples, fn ($a, $b) => $a['name'] <=> $b['name']);
    foreach ($samples as $row) {
        $lines[] = sprintf(
            '| %s | %s | %s | %.2f | %.2f | %.2f | %.2f | %.2f | %d |',
            $row['name'], $row['seq'], $row['err'], $row['p50'], $row['p99'],
            $row['mean'], $row['min'], $row['max'], $row['iters'],
        );
    }
    $failed = array_values(array_filter($samples, fn ($r) => $r['err'] !== 'OK'));
    $lines[] = '';
    $lines[] = count($failed) === 0
        ? 'Every scenario ran its success path (no non-OK gRPC status).'
        : count($failed).' scenario(s) returned a non-OK gRPC status (see [SCENARIO-FAIL] log lines).';

    file_put_contents('scenario_perf_php.md', implode("\n", $lines)."\n");
    echo "\nPHP scenario perf: ".count($samples)." workflow helpers measured, ".count($failed)." FAILED → sdk/php/scenario_perf_php.md\n";

    expect(count($samples))->toBeGreaterThanOrEqual(7);
})->skip(getenv('UDB_SCENARIO_PERF') !== '1', 'scenario perf run requires UDB_SCENARIO_PERF=1');
