<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Support\AuthzCache;
use Udb\Core\Authz\Services\V1\Decision;

/*
 * AuthzCache pure-logic tests.
 *
 * The cache key builder is pure PHP (no protobuf) and is always exercised.
 * The TTL hit/expiry tests construct a real {@see Decision} message, which
 * needs ext-protobuf; those cases skip gracefully when the extension is
 * absent (as in the lint-only CI image) so the suite still runs there.
 */

it('builds a stable, scope-order-independent cache key', function () {
    $a = AuthzCache::key('u1', 't1', 'p1', 'res', 'read', 'purpose', ['b', 'a']);
    $b = AuthzCache::key('u1', 't1', 'p1', 'res', 'read', 'purpose', ['a', 'b']);
    expect($a)->toBe($b);

    $different = AuthzCache::key('u1', 't1', 'p1', 'res', 'write', 'purpose', ['a', 'b']);
    expect($different)->not->toBe($a);
});

it('differentiates keys by principal / tenant / project / resource', function () {
    $base = AuthzCache::key('u1', 't1', 'p1', 'res', 'read', '', []);
    expect(AuthzCache::key('u2', 't1', 'p1', 'res', 'read', '', []))->not->toBe($base);
    expect(AuthzCache::key('u1', 't2', 'p1', 'res', 'read', '', []))->not->toBe($base);
    expect(AuthzCache::key('u1', 't1', 'p2', 'res', 'read', '', []))->not->toBe($base);
    expect(AuthzCache::key('u1', 't1', 'p1', 'other', 'read', '', []))->not->toBe($base);
});

it('returns a live decision within TTL and evicts after it expires', function () {
    if (! extension_loaded('protobuf')) {
        test()->markTestSkipped('ext-protobuf not installed; cannot construct a Decision message.');
    }

    $cache = new AuthzCache(defaultTtlSeconds: 30);
    $decision = (new Decision())->setAllowed(true)->setCacheTtlSeconds(1);

    $cache->put('k', $decision);
    expect($cache->get('k'))->toBe($decision)
        ->and($cache->size())->toBe(1);

    // Force expiry without sleeping by replaying the eviction path: TTL of 1s.
    sleep(1);
    expect($cache->get('k'))->toBeNull()
        ->and($cache->size())->toBe(0);
})->skip(! extension_loaded('protobuf'), 'requires ext-protobuf');

it('does not cache a zero-TTL decision by default', function () {
    if (! extension_loaded('protobuf')) {
        test()->markTestSkipped('ext-protobuf not installed; cannot construct a Decision message.');
    }

    $cache = new AuthzCache();
    $decision = (new Decision())->setAllowed(true)->setCacheTtlSeconds(0);
    $cache->put('k', $decision);
    expect($cache->get('k'))->toBeNull()
        ->and($cache->size())->toBe(0);
})->skip(! extension_loaded('protobuf'), 'requires ext-protobuf');

it('uses the explicit fallback TTL for zero-TTL decisions', function () {
    if (! extension_loaded('protobuf')) {
        test()->markTestSkipped('ext-protobuf not installed; cannot construct a Decision message.');
    }

    $cache = new AuthzCache(defaultTtlSeconds: 30);
    $decision = (new Decision())->setAllowed(true)->setCacheTtlSeconds(0);
    $cache->put('k', $decision);
    expect($cache->get('k'))->toBe($decision)
        ->and($cache->size())->toBe(1);
})->skip(! extension_loaded('protobuf'), 'requires ext-protobuf');

it('prefers the server-controlled TTL over the default', function () {
    if (! extension_loaded('protobuf')) {
        test()->markTestSkipped('ext-protobuf not installed; cannot construct a Decision message.');
    }

    // Default TTL is 0 (non-cacheable) but the server TTL of 30s should win.
    $cache = new AuthzCache(defaultTtlSeconds: 0);
    $decision = (new Decision())->setAllowed(true)->setCacheTtlSeconds(30);
    $cache->put('k', $decision);
    expect($cache->get('k'))->toBe($decision);
})->skip(! extension_loaded('protobuf'), 'requires ext-protobuf');

it('flush() drops every entry', function () {
    if (! extension_loaded('protobuf')) {
        test()->markTestSkipped('ext-protobuf not installed; cannot construct a Decision message.');
    }

    $cache = new AuthzCache(defaultTtlSeconds: 30);
    $cache->put('a', (new Decision())->setCacheTtlSeconds(30));
    $cache->put('b', (new Decision())->setCacheTtlSeconds(30));
    expect($cache->size())->toBe(2);
    $cache->flush();
    expect($cache->size())->toBe(0);
})->skip(! extension_loaded('protobuf'), 'requires ext-protobuf');
