<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services\WebRtc;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Webrtc\Services\V1\SignalingServiceClient;

/**
 * Thin accessor over the generated {@see SignalingServiceClient}. The single
 * RPC, `Signal`, is a BIDIRECTIONAL STREAM (SDP offer/answer + ICE exchange),
 * which the project's unary {@see UdbProject::invoke()} helper does not model.
 * Rather than fake a unary surface, this wrapper opens the raw bidi call with
 * the shared metadata applied and hands the caller the {@see \Grpc\BidiStreamingCall}
 * to drive `write()` / `read()` directly.
 *
 * TODO (streaming ergonomics): a higher-level loop/iterator helper over the
 * stream (auto-encode {@see \Udb\Core\Webrtc\Services\V1\SignalRequest}, yield
 * decoded {@see \Udb\Core\Webrtc\Services\V1\SignalResponse}) is intentionally
 * deferred — it needs a live duplex transport to design against, and the grpc
 * ext is not present in this build.
 */
final class SignalingApi
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly SignalingServiceClient $stub,
    ) {
    }

    /** The raw generated stub. */
    public function client(): SignalingServiceClient
    {
        return $this->stub;
    }

    /**
     * Open the bidirectional signaling stream with the project's shared
     * metadata applied. Caller drives it: `$call->write($signalRequest)`,
     * `$call->read()`, `$call->writesDone()`, `$call->getStatus()`.
     *
     * @param  array<string,mixed>  $options  extra gRPC call options (merged last)
     * @return \Grpc\BidiStreamingCall
     */
    public function signal(?UdbMetadata $metadata = null, array $options = [])
    {
        $meta = $this->project->metadata($metadata);

        return $this->stub->Signal($meta->toGrpcMetadata(), $options);
    }
}
