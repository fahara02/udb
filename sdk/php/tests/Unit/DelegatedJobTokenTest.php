<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\DelegatedJobToken;
use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\UdbAuthClient;
use Udb\Core\Authn\Services\V1\ExchangeTokenForJobResponse;
use Udb\Entity\V1\ErrorDetail;
use Udb\Entity\V1\ErrorKind;

/*
 * Delegated job tokens without a broker: the request
 * UdbAuthClient::exchangeTokenForJob() sends, the token it returns, the local
 * refusals, and the token-refusal reason helpers on UdbRpcException. Parity
 * with the Go/Python/TS delegated-token unit tests.
 */

function refusalWithReason(string $reason): UdbRpcException
{
    $detail = (new ErrorDetail())->setReason($reason)->setKind(ErrorKind::ERROR_KIND_POLICY);

    return UdbRpcException::fromGrpcStatus([
        'code' => 16, // UNAUTHENTICATED
        'details' => 'refused',
        'metadata' => ['udb-error-detail-bin' => [$detail->serializeToString()]],
    ], 'AuthnService/Authenticate');
}

it('builds the request with the subject token, purpose, job and ttl', function () {
    $request = UdbAuthClient::jobTokenRequest('user.jwt', 'export', 'export-42', 600);

    expect($request->getSubjectToken())->toBe('user.jwt')
        ->and($request->getPurpose())->toBe('export')
        ->and($request->getJobId())->toBe('export-42')
        ->and((int) $request->getTtlSeconds())->toBe(600)
        ->and((int) UdbAuthClient::jobTokenRequest('t', 'export', 'j', 0)->getTtlSeconds())->toBe(0);
});

it('refuses blank arguments and a negative ttl locally', function (string $subject, string $purpose, string $jobId, int $ttl) {
    expect(fn () => UdbAuthClient::jobTokenRequest($subject, $purpose, $jobId, $ttl))
        ->toThrow(UdbConfigurationException::class);
})->with([
    'blank subject token' => [' ', 'export', 'j', 0],
    'blank purpose' => ['t', '', 'j', 0],
    'blank job id' => ['t', 'export', '', 0],
    'negative ttl' => ['t', 'export', 'j', -1],
]);

it('maps the response to a DelegatedJobToken and refuses an empty token', function () {
    $response = (new ExchangeTokenForJobResponse())
        ->setAccessToken('delegated.jwt')
        ->setJti('dlg_1')
        ->setExpiresAtUnix(1_900_000_000)
        ->setSubject('user-1')
        ->setActor('spiffe://acme/exporter')
        ->setPurpose('export')
        ->setJobId('export-42')
        ->setScopes(['udb:read'])
        ->setTenantId('tenant-1')
        ->setProjectId('billing');

    $token = UdbAuthClient::delegatedJobToken($response, 'export-42');

    expect($token)->toEqual(new DelegatedJobToken(
        accessToken: 'delegated.jwt',
        jti: 'dlg_1',
        expiresAtUnix: 1_900_000_000,
        subject: 'user-1',
        actor: 'spiffe://acme/exporter',
        purpose: 'export',
        jobId: 'export-42',
        scopes: ['udb:read'],
        tenantId: 'tenant-1',
        projectId: 'billing',
    ))
        ->and(print_r($token, true))->not->toContain('delegated.jwt');

    expect(fn () => UdbAuthClient::delegatedJobToken(new ExchangeTokenForJobResponse(), 'export-42'))
        ->toThrow(UdbConfigurationException::class);
});

it('tells expired and revoked bearers apart by the typed reason', function () {
    $expired = refusalWithReason('UDB_TOKEN_EXPIRED');
    $revoked = refusalWithReason('UDB_TOKEN_REVOKED');

    expect($expired->status)->toBe(16)
        ->and($expired->reason())->toBe('UDB_TOKEN_EXPIRED')
        ->and($expired->isTokenExpired())->toBeTrue()
        ->and($expired->isTokenRevoked())->toBeFalse()
        ->and($revoked->isTokenRevoked())->toBeTrue()
        ->and($revoked->isTokenExpired())->toBeFalse()
        ->and(refusalWithReason('UDB_AUTHENTICATION_FAILED')->isTokenExpired())->toBeFalse();

    // The message text is never matched.
    $textOnly = new UdbRpcException(status: 16, details: 'UDB_TOKEN_EXPIRED');
    expect($textOnly->isTokenExpired())->toBeFalse()
        ->and($textOnly->reason())->toBe('');
});
