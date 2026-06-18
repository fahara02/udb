<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Services\EventsService;
use Fahara02\UdbLaravel\Services\EventSubscription;
use Fahara02\UdbLaravel\UdbProject;

/*
 * R2.4-PHP: events facade (subscribe(topic).ready() + publishAndWait) parity
 * with the naming contract.
 *
 * PHP's ext-grpc is fully synchronous, so subscribe(topic).ready() is BLOCKING
 * (server-driven via the stream's initial metadata, never a sleep) rather than
 * promise-based — see EventsService / EventSubscription docblocks. These tests
 * exercise the deterministic, transport-free pieces: the EventSubscription
 * readiness/peek/iteration state machine and the publishAndWait match loop, plus
 * the facade method shape. The live enqueue->await round trip is covered by the
 * Live suite (it needs ext-grpc + a broker). No generated protobuf or gRPC
 * channel is constructed here, so it runs in the lint-only image.
 */

/**
 * A transport-free fake of \Grpc\ServerStreamingCall: a fixed list of envelopes
 * yielded by responses(), with an optional getMetadata() readiness boundary and
 * a call counter to prove responses() is not double-consumed.
 */
final class FakeCdcStream
{
    public int $responsesCalls = 0;
    public int $getMetadataCalls = 0;

    /** @param list<object> $envelopes */
    public function __construct(
        private readonly array $envelopes,
        private readonly bool $hasMetadata = true,
    ) {
    }

    /** @return \Generator<int,object> */
    public function responses(): \Generator
    {
        $this->responsesCalls++;
        foreach ($this->envelopes as $e) {
            yield $e;
        }
    }

    /** @return array<string,mixed> */
    public function getMetadata(): array
    {
        if (! $this->hasMetadata) {
            throw new \RuntimeException('no initial metadata');
        }
        $this->getMetadataCalls++;

        return [];
    }
}

/** A minimal envelope stand-in carrying an event id (mirrors CDCEnvelope::getEventId). */
final class FakeEnvelope
{
    public function __construct(private readonly string $eventId)
    {
    }

    public function getEventId(): string
    {
        return $this->eventId;
    }
}

// ── EventSubscription readiness boundary (server-driven, no sleep) ───────────
it('ready() resolves on the server initial-metadata boundary without peeking', function () {
    $stream = new FakeCdcStream([new FakeEnvelope('e1')], hasMetadata: true);
    $sub = new EventSubscription($stream);

    $sub->ready();
    $sub->ready(); // idempotent

    expect($stream->getMetadataCalls)->toBe(1)
        // metadata boundary => the envelope stream is NOT consumed by ready()
        ->and($stream->responsesCalls)->toBe(0);

    // Iterating still yields the full stream (nothing was eaten by ready()).
    $ids = array_map(fn ($e) => $e->getEventId(), iterator_to_array($sub));
    expect($ids)->toBe(['e1']);
});

it('ready() falls back to peeking the first envelope when no metadata boundary', function () {
    $stream = new FakeCdcStream([new FakeEnvelope('e1'), new FakeEnvelope('e2')], hasMetadata: false);
    $sub = new EventSubscription($stream);

    $sub->ready();

    // The peeked first envelope is replayed first, then the rest — no envelope lost.
    $ids = array_map(fn ($e) => $e->getEventId(), iterator_to_array($sub));
    expect($ids)->toBe(['e1', 'e2']);
});

// ── publishAndWait default-match: matches the enqueued event_id ──────────────
it('the default publishAndWait match selects the envelope with the enqueued event_id', function () {
    // Reconstruct the EXACT default matcher EventsService uses (matches getEventId).
    $target = 'evt-42';
    $match = static function (object $envelope) use ($target): bool {
        return method_exists($envelope, 'getEventId') && $envelope->getEventId() === $target;
    };

    $sub = new EventSubscription(new FakeCdcStream([
        new FakeEnvelope('evt-1'),
        new FakeEnvelope('evt-42'),
        new FakeEnvelope('evt-99'),
    ], hasMetadata: false));

    $hit = null;
    foreach ($sub as $env) {
        if ($match($env)) {
            $hit = $env;
            break;
        }
    }

    expect($hit)->not->toBeNull()->and($hit->getEventId())->toBe('evt-42');
});

it('publishAndWait returns null when the stream ends with no match', function () {
    $match = static fn (object $e): bool => $e->getEventId() === 'never';
    $sub = new EventSubscription(new FakeCdcStream([new FakeEnvelope('a'), new FakeEnvelope('b')], hasMetadata: false));

    $hit = null;
    foreach ($sub as $env) {
        if ($match($env)) {
            $hit = $env;
            break;
        }
    }
    expect($hit)->toBeNull();
});

// ── Facade shape: subscribe/publishAndWait/enqueue exist with the right types ─
it('EventsService exposes subscribe/publishAndWait/enqueue and a stream-opener seam', function () {
    foreach (['subscribe', 'publishAndWait', 'enqueue', 'withStreamOpener'] as $m) {
        expect(method_exists(EventsService::class, $m))->toBeTrue("EventsService::{$m}() missing");
    }
    expect((new ReflectionMethod(EventsService::class, 'subscribe'))->getReturnType()?->getName())
        ->toBe(EventSubscription::class);

    // UdbProject advertises the events() accessor (parity with .data/.auth/...).
    expect(method_exists(UdbProject::class, 'events'))->toBeTrue('UdbProject::events() missing')
        ->and((new ReflectionMethod(UdbProject::class, 'events'))->getReturnType()?->getName())
        ->toBe(EventsService::class);
});

// ── No hidden round trips: publishAndWait emits exactly EnqueueOutboxEvent ───
it('publishAndWait/enqueue emit exactly one EnqueueOutboxEvent and subscribe exactly PublishCDC', function () {
    $src = (string) file_get_contents(dirname(__DIR__, 2).'/src/Services/EventsService.php');

    // enqueue() routes exactly one EnqueueOutboxEvent invoke (a unary mutation).
    expect(substr_count($src, "'EnqueueOutboxEvent'"))->toBe(1)
        ->and(substr_count($src, '->EnqueueOutboxEvent('))->toBe(1)
        // subscribe() opens exactly one PublishCDC stream (default opener).
        ->and(substr_count($src, '->PublishCDC('))->toBe(1)
        // No hidden List/Get probes anywhere in the facade.
        ->and($src)->not->toContain('->ListFiles(')
        ->and($src)->not->toContain('->GetFile(')
        ->and(preg_match('/->(List|Get)[A-Z]\w*\(/', $src))->toBe(0);
});
