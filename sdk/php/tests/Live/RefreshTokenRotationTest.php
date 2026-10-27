<?php

declare(strict_types=1);

// Live refresh-token rotation conformance. Skipped unless UDB_LIVE_SDK_TESTS=1;
// CI's "SDK refresh-token rotation (live)" job runs it against the broker it
// just built, on every push to main.
//
// The broker rotates the refresh token on every AuthnService.RefreshToken call
// and each one works once. An SDK caller that kept the token it logged in with
// refreshed once and then failed with UNAUTHENTICATED "invalid credential" — a
// production outage no single-refresh test could catch. The PHP SDK keeps no
// token store: refresh is GeneratedClient::refreshToken, and the caller
// contract is that each refresh presents the refresh token the previous one
// returned. This test chains it exactly that way.

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Authn\Entity\V1\TokenType;
use Udb\Core\Authn\Services\V1\RefreshTokenRequest;
use Udb\Core\Authn\Services\V1\ValidateTokenRequest;

function refreshRotationEnv(string $name, ?string $fallback = null): string
{
    $value = trim((string) getenv($name));
    if ($value !== '') {
        return $value;
    }
    if ($fallback !== null) {
        return $fallback;
    }
    throw new RuntimeException("{$name} is required when UDB_LIVE_SDK_TESTS=1");
}

it('live refresh token rotation survives repeated refreshes', function () {
    $target = refreshRotationEnv('UDB_GRPC_TARGET');
    $authTarget = refreshRotationEnv('UDB_AUTH_GRPC_TARGET', $target);
    $username = refreshRotationEnv('UDB_LIVE_USERNAME');
    $password = refreshRotationEnv('UDB_LIVE_PASSWORD');

    $project = UdbProject::connect([
        'target' => $target,
        'authTarget' => $authTarget,
        'tenantId' => refreshRotationEnv('UDB_LIVE_TENANT', 'sdk-live'),
        'projectId' => refreshRotationEnv('UDB_LIVE_PROJECT', 'default'),
        'purpose' => 'php.live.refresh-rotation',
        'correlationId' => 'php-live-refresh-rotation',
        'serviceIdentity' => 'php.sdk.live',
        'deadline_ms' => 10_000,
    ]);
    // Adopt the canonical tenant the login JWT carries; authenticated calls
    // must assert it rather than the human tenant code.
    $project->loginAndAdoptTenant($username, $password);
    // The refresh chain starts at a login's refresh token.
    $login = $project->auth()->login($username, $password);
    $first = $login->getRefreshToken();
    expect($login->getAccessToken())->not->toBe('', 'login returned no access token');
    expect($first)->not->toBe('', 'login returned no refresh token');
    $project->setCredentials($login->getAccessToken());

    $authn = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000, 'retry' => ['max_attempts' => 1]]);
    $previous = $first;
    for ($i = 1; $i <= 3; $i++) {
        $refreshed = $authn->refreshToken(
            (new RefreshTokenRequest())->setRefreshToken($previous),
            $project->metadata(),
        );
        $next = $refreshed->getRefreshToken();
        expect($refreshed->getAccessToken())->not->toBe('', "refresh {$i} returned no access token");
        expect($next)->not->toBe('', "refresh {$i} returned no refresh token");
        expect($next)->not->toBe(
            $previous,
            "refresh {$i}: the refresh token did not change — the broker stopped rotating it",
        );

        // An authenticated call carrying the refreshed bearer.
        $project->setCredentials($refreshed->getAccessToken());
        $validated = $authn->validateToken(
            (new ValidateTokenRequest())
                ->setToken($refreshed->getAccessToken())
                ->setTokenType(TokenType::TOKEN_TYPE_JWT_ACCESS),
            $project->metadata(),
        );
        expect($validated->getValid())->toBeTrue("refresh {$i}: the refreshed access token does not validate");
        $previous = $next;
    }

    // The first refresh token was spent three rotations ago: the broker must
    // refuse it. (Replaying the immediately-previous token instead would trip
    // reuse detection and revoke every session of this shared live user.)
    $refusedWith = null;
    try {
        $authn->refreshToken((new RefreshTokenRequest())->setRefreshToken($first), $project->metadata());
    } catch (UdbRpcException $e) {
        $refusedWith = $e->status;
    }
    expect($refusedWith)->toBe(16, 'a spent refresh token must be refused with UNAUTHENTICATED (16)');
})->skip(getenv('UDB_LIVE_SDK_TESTS') !== '1', 'requires live UDB broker');
