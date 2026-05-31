<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Exceptions;

use RuntimeException;

/**
 * Root exception type for the UDB Laravel SDK. Every other SDK
 * exception extends this so callers can `catch (UdbException $e)`
 * to gate all UDB-related failures without ambiguity.
 *
 * The SDK never throws a bare `RuntimeException`; it always
 * narrows to one of the typed subclasses below.
 */
class UdbException extends RuntimeException
{
}
