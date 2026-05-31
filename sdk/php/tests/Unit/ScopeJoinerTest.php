<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Support\ScopeJoiner;

it('joins a list of scopes with commas', function () {
    expect(ScopeJoiner::join(['udb:read', 'udb:write', 'admin']))
        ->toBe('udb:read,udb:write,admin');
});

it('returns an empty string for an empty scope list', function () {
    expect(ScopeJoiner::join([]))->toBe('');
});

it('trims whitespace around each scope', function () {
    expect(ScopeJoiner::join([' udb:read ', "\tudb:write\n"]))
        ->toBe('udb:read,udb:write');
});

it('filters out empty strings (env-pasted lists frequently produce them)', function () {
    expect(ScopeJoiner::join(['udb:read', '', '   ', 'admin']))
        ->toBe('udb:read,admin');
});

it('round-trips through split + join', function () {
    $original = ['udb:cdc:read', 'udb:select', 'udb:upsert'];
    $joined = ScopeJoiner::join($original);
    $split = ScopeJoiner::split($joined);
    expect($split)->toBe($original);
});

it('split() returns an empty list for an empty value', function () {
    expect(ScopeJoiner::split(''))->toBe([]);
});

it('split() drops empty entries from doubled commas', function () {
    expect(ScopeJoiner::split('udb:read,,udb:write'))->toBe(['udb:read', 'udb:write']);
});
