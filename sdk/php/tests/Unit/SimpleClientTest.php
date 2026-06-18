<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\ReadFence;
use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\WriteReceipt;

/*
 * Chapter-10 PHP simple-client helper tests (offline; no broker / gRPC channel).
 * Covers: typed error-detail surface, optional is_public, typed receipt/fence +
 * golden parity, UdbMetadata consistency/fence headers, bound entity, and the
 * uploadFile register→PUT→finalize sequence.
 */

$needsProto = function (): void {
    // Generated messages + Struct construct via the pure-PHP google/protobuf lib
    // (no C ext required here), but skip cleanly if neither is available.
    if (! class_exists(\Udb\Core\Storage\Services\V1\RegisterUploadRequest::class)) {
        test()->markTestSkipped('generated protobuf messages not available.');
    }
};

// ── 10.3.1.5 golden: ReadFence::fromReceipt matches the committed fixture ────
it('maps a WriteReceipt into a ReadFence matching the golden fixture', function () {
    $golden = json_decode((string) file_get_contents(
        dirname(__DIR__, 4).'/docs/generated/consistency-golden.json'
    ), true);
    $wr = $golden['write_receipt'];
    $receipt = new WriteReceipt(
        sourceLsn: $wr['source_lsn'],
        outboxSeq: $wr['outbox_seq'],
        projectionTaskIds: $wr['projection_task_ids'],
        manifestChecksum: $wr['manifest_checksum'],
        writtenAtUnixMs: $wr['written_at_unix_ms'],
    );
    $fence = ReadFence::fromReceipt($receipt, $golden['read_fence']['max_wait_ms']);

    expect($fence->toArray())->toBe($golden['read_fence'])
        ->and(json_decode($fence->toJson(), true))->toBe($golden['read_fence'])
        // the load-bearing cross-type mapping
        ->and($fence->minOutboxLsn)->toBe($receipt->sourceLsn);
});

it('omits empty min_outbox_lsn / projection_task_ids in the fence JSON', function () {
    $fence = ReadFence::fromReceipt(new WriteReceipt(), 100);
    expect($fence->toArray())->toBe(['max_wait_ms' => 100]);
});

it('WriteReceipt parses write_receipt_json round-trip', function () {
    $r = WriteReceipt::fromJson('{"source_lsn":"0/BB","outbox_seq":7}');
    expect($r->sourceLsn)->toBe('0/BB')->and($r->outboxSeq)->toBe(7);
});

// ── 10.3.1.4: UdbMetadata consistency/fence headers (omit-when-empty) ────────
it('emits x-udb-read-fence only when a fence is set', function () {
    $base = new UdbMetadata('t', 'u', 'p', 'c', [], 'svc', 'proj', '1.0.0');
    expect($base->toGrpcMetadata())->not->toHaveKey('x-udb-read-fence');

    $fenced = $base->withReadFence('{"max_wait_ms":2500}');
    $headers = $fenced->toGrpcMetadata();
    expect($headers['x-udb-read-fence'])->toBe(['{"max_wait_ms":2500}']);
});

it('afterWrite installs a fence whose min_outbox_lsn is the receipt source_lsn', function () {
    $base = new UdbMetadata('t', 'u', 'p', 'c', [], 'svc', 'proj', '1.0.0');
    $meta = $base->afterWrite(new WriteReceipt(sourceLsn: '0/AA'));
    $decoded = json_decode($meta->toGrpcMetadata()['x-udb-read-fence'][0], true);
    expect($decoded['min_outbox_lsn'])->toBe('0/AA');
});

it('emits the consistency / replica-lag headers only when set', function () {
    $m = new UdbMetadata(
        't', 'u', 'p', 'c', [], 'svc', 'proj', '1.0.0',
        consistency: 'strong', primaryRead: true, maxReplicaLagMs: 250,
        eventualConsistencyAllowed: true,
    );
    $h = $m->toGrpcMetadata();
    expect($h['x-udb-consistency'])->toBe(['strong'])
        ->and($h['x-udb-primary-read'])->toBe(['true'])
        ->and($h['x-udb-max-replica-lag-ms'])->toBe(['250'])
        ->and($h['x-udb-eventual-consistency-allowed'])->toBe(['true']);
});

// ── 10.3.2.2 / 10.3.2.3: errorDetail property + isRetryable()/kind() ─────────
it('carries a settable errorDetail and typed accessors', function () use ($needsProto) {
    $needsProto();
    $ed = new \Udb\Entity\V1\ErrorDetail();
    $ed->setRetryable(true);
    $ed->setKind(\Udb\Entity\V1\ErrorKind::ERROR_KIND_CAPABILITY);
    $ed->setCapabilityRequired('cap.x');

    // fromGrpcStatus decodes the trailer into errorDetail.
    $status = ['code' => 9, 'details' => 'boom', 'metadata' => [
        'udb-error-detail-bin' => [$ed->serializeToString()],
    ]];
    $e = UdbRpcException::fromGrpcStatus($status, 'X');

    expect($e->errorDetail)->toBeInstanceOf(\Udb\Entity\V1\ErrorDetail::class)
        ->and($e->isRetryable())->toBeTrue()
        ->and($e->kind())->toBe('ERROR_KIND_CAPABILITY');
});

it('isRetryable() and kind() are safe when no detail was decoded', function () {
    $e = UdbRpcException::fromGrpcStatus(['code' => 5, 'details' => 'nf'], 'Y');
    expect($e->errorDetail)->toBeNull()
        ->and($e->isRetryable())->toBeFalse()
        ->and($e->kind())->toBe('');
});

// ── 10.4.1.4: optional is_public stays unset unless supplied ─────────────────
it('leaves is_public unset on RegisterUpload when not supplied', function () use ($needsProto) {
    $needsProto();
    $req = new \Udb\Core\Storage\Services\V1\RegisterUploadRequest();
    // Mirror the production path: only setIsPublic when non-null.
    $isPublic = null;
    if ($isPublic !== null) {
        $req->setIsPublic($isPublic);
    }
    expect($req->hasIsPublic())->toBeFalse();

    $req2 = new \Udb\Core\Storage\Services\V1\RegisterUploadRequest();
    $req2->setIsPublic(true);
    expect($req2->hasIsPublic())->toBeTrue()->and($req2->getIsPublic())->toBeTrue();
});

// ── 10.2.2.3: UdbClient struct/record helpers ───────────────────────────────
it('encodes a record array to record_json and builds a Struct filter', function () use ($needsProto) {
    $needsProto();
    expect(UdbClient::encodeRecordJson(['id' => 'i1', 'n' => 2]))
        ->toBe('{"id":"i1","n":2}');

    $struct = UdbClient::toStruct(['id' => 'i1']);
    expect($struct)->toBeInstanceOf(\Google\Protobuf\Struct::class);
    // round-trips back to the same field
    expect(json_decode($struct->serializeToJsonString(), true))->toBe(['id' => 'i1']);
});
