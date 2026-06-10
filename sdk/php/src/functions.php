<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

if (! \function_exists('Fahara02\\UdbLaravel\\createUdb')) {
    /**
     * Construct a {@see UdbProject} from a single config bag — the ergonomic
     * entry point for non-Laravel callers (and Laravel callers who prefer an
     * explicit object to the static facade).
     *
     *   $udb = \Fahara02\UdbLaravel\createUdb(['target' => '127.0.0.1:50051', ...]);
     *
     * @param  array<string,mixed>  $config  See {@see UdbProject} for keys.
     */
    function createUdb(array $config): UdbProject
    {
        /** @var \Fahara02\UdbLaravel\UdbProject */
        return new UdbProject($config);
    }
}
