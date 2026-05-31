<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Exceptions;

/**
 * Thrown at construction / boot when the SDK is misconfigured —
 * missing endpoint, TLS enabled without a root cert, missing
 * gRPC extension, etc. These are programmer / operator errors,
 * not runtime RPC failures.
 *
 * Distinguishing this from `UdbRpcException` lets observability
 * route configuration errors to the on-call alert channel
 * (you broke the deployment) while RPC errors get the regular
 * application-error channel (the broker says no).
 */
final class UdbConfigurationException extends UdbException
{
}
