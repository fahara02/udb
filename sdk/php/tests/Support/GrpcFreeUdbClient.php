<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Tests\Support;

use Fahara02\UdbLaravel\UdbClient;

/**
 * Test-only UdbClient that skips the production ext-grpc constructor guard.
 *
 * Feature tests exercise Laravel container binding and request-context metadata,
 * not actual RPC transport. RPC-oriented tests remain skipped when ext-grpc is
 * unavailable.
 */
final class GrpcFreeUdbClient extends UdbClient
{
    public function __construct()
    {
        // Parent construction validates ext-grpc and endpoint config. The tests
        // using this double only call bindContext()/context().
    }
}
