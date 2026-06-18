<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

/**
 * A live CDC subscription handle returned by {@see EventsService::subscribe()}.
 *
 * Wraps the `PublishCDC` server-stream (a `\Grpc\ServerStreamingCall`, or any
 * object exposing `responses()` / `getMetadata()`). Implements
 * {@see \IteratorAggregate} so callers can `foreach` the live `CDCEnvelope`s.
 *
 * {@see ready()} blocks until the first server signal — the server's INITIAL
 * METADATA boundary (`getMetadata()`) when available — without a `sleep()`.
 * Because PHP's ext-grpc is fully synchronous there is no promise/event-loop
 * readiness; `ready()` is therefore blocking by design (documented limitation),
 * but it IS server-driven, never a timer.
 */
final class EventSubscription implements \IteratorAggregate
{
    private bool $ready = false;

    /** Buffered first envelope when readiness fell back to a peek. */
    private ?object $peeked = null;
    private bool $hasPeeked = false;

    /**
     * The stream's `responses()` generator, fetched at most ONCE and reused so
     * a peek in {@see ready()} and later iteration drain the SAME underlying
     * server stream (calling `responses()` twice on a real gRPC call would
     * restart/duplicate it). Null until first needed.
     *
     * @var null|\Iterator<int,object>
     */
    private ?\Iterator $responses = null;

    public function __construct(private readonly object $stream)
    {
    }

    /** Lazily fetch + memoize the single live responses() generator. */
    private function responses(): ?\Iterator
    {
        if ($this->responses === null && method_exists($this->stream, 'responses')) {
            $gen = $this->stream->responses();
            $this->responses = $gen instanceof \Iterator ? $gen : new \ArrayIterator((array) $gen);
        }

        return $this->responses;
    }

    /**
     * Block until the stream is ready (the server has accepted it). Prefers the
     * initial-metadata boundary (`getMetadata()`); falls back to peeking the
     * first streamed envelope. Idempotent — repeated calls are no-ops.
     */
    public function ready(): void
    {
        if ($this->ready) {
            return;
        }

        // Server's initial metadata is the readiness boundary (no timer).
        if (method_exists($this->stream, 'getMetadata')) {
            try {
                $this->stream->getMetadata();
                $this->ready = true;

                return;
            } catch (\Throwable) {
                // Fall through to a peek when the call has no metadata accessor.
            }
        }

        if (! $this->hasPeeked) {
            $responses = $this->responses();
            if ($responses !== null) {
                $responses->rewind();
                if ($responses->valid()) {
                    $this->peeked = $responses->current();
                    $responses->next();
                }
                $this->hasPeeked = true;
            }
        }
        $this->ready = true;
    }

    /** Raw underlying stream/call, for advanced callers (status/cancel). */
    public function stream(): object
    {
        return $this->stream;
    }

    /**
     * Iterate the live `CDCEnvelope`s. Replays any envelope peeked by
     * {@see ready()} first, then drains the stream's `responses()`.
     *
     * @return \Generator<int,object>
     */
    public function getIterator(): \Generator
    {
        if ($this->hasPeeked) {
            $first = $this->peeked;
            $this->peeked = null;
            $this->hasPeeked = false;
            if ($first !== null) {
                yield $first;
            }
        }
        $responses = $this->responses();
        if ($responses !== null) {
            // Resume from wherever ready()'s peek left off (the same generator).
            for (; $responses->valid(); $responses->next()) {
                yield $responses->current();
            }
        }
    }
}
