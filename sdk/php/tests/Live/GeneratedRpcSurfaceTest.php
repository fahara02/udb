<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\UdbAuthClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Udb\Core\Authn\Services\V1\LoginRequest;
use Udb\Core\Authn\Services\V1\RefreshTokenRequest;
use Udb\Entity\V1\CapabilitiesRequest;

if (getenv('UDB_LIVE_SDK_TESTS') !== '1') {
    test('live generated RPC surface')->skip('requires live UDB broker');
    return;
}

function liveEnv(string $name, ?string $fallback = null): string
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

function liveMeta(string $bearerToken = ''): UdbMetadata
{
    return new UdbMetadata(
        tenantId: liveEnv('UDB_LIVE_TENANT', 'sdk-live'),
        userId: '',
        purpose: 'php.live.conformance',
        correlationId: 'php-live-conformance',
        // No client-asserted scopes: admin authority comes from the Login JWT
        // (broker derives scopes from the validated bearer; header/body scopes are
        // ignored when a JWT verifier is configured). The real production path.
        scopes: [],
        serviceIdentity: 'php.sdk.live',
        projectId: liveEnv('UDB_LIVE_PROJECT', 'default'),
        clientCatalogVersion: '1.0.0',
        bearerToken: $bearerToken,
    );
}

function isFatalLiveStatus(int $code): bool
{
    // DEADLINE_EXCEEDED is NOT a mount failure: an unmounted RPC returns
    // UNIMPLEMENTED instantly, so a timeout means the server accepted the call and
    // is processing/blocking (e.g. PublishCDC is an open-ended CDC subscription
    // stream that legitimately blocks waiting for events).
    return in_array($code, [
        12, // UNIMPLEMENTED
        14, // UNAVAILABLE
        2,  // UNKNOWN
    ], true);
}

function assertLiveStatusMounted(string $label, mixed $status): void
{
    $code = is_object($status) ? (int) ($status->code ?? -1) : (int) ($status['code'] ?? -1);
    $details = is_object($status) ? (string) ($status->details ?? '') : (string) ($status['details'] ?? '');
    expect(isFatalLiveStatus($code))
        ->toBeFalse("{$label} did not reach an implemented live RPC: code={$code} details={$details}");
}

function requestFor(ReflectionMethod $method): object
{
    $params = $method->getParameters();
    if (count($params) === 0) {
        throw new RuntimeException("{$method->getName()} has no request parameter");
    }
    $type = $params[0]->getType();
    if (! $type instanceof ReflectionNamedType || $type->isBuiltin()) {
        throw new RuntimeException("{$method->getName()} first parameter is not a generated request type");
    }
    $class = $type->getName();
    return new $class();
}

function generatedStubMethods(object $stub): array
{
    $out = [];
    $ref = new ReflectionClass($stub);
    foreach ($ref->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
        if ($method->getDeclaringClass()->getName() !== $ref->getName()) {
            continue;
        }
        if ($method->isConstructor()) {
            continue;
        }
        $out[] = $method;
    }
    return $out;
}

function stubAccessors(GeneratedClient $data, GeneratedClient $authGenerated): array
{
    $out = [];
    $ref = new ReflectionClass(GeneratedClient::class);
    foreach ($ref->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
        if (! str_ends_with($method->getName(), 'Stub')) {
            continue;
        }
        $client = $method->getName() === 'DataBrokerStub' ? $data : $authGenerated;
        $out[$method->getName()] = $method->invoke($client);
    }
    return $out;
}

it('covers the live generated RPC surface', function () {
    $target = liveEnv('UDB_GRPC_TARGET');
    $authTarget = liveEnv('UDB_AUTH_GRPC_TARGET', $target);
    $meta = liveMeta();

    $openAuthGenerated = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000, 'retry' => ['max_attempts' => 1]]);
    $openAuthGenerated->bindContext($meta);
    $login = $openAuthGenerated->login(
        (new LoginRequest())
            ->setUsername(liveEnv('UDB_LIVE_USERNAME'))
            ->setPassword(liveEnv('UDB_LIVE_PASSWORD'))
            ->setTenantHint($meta->tenantId)
            ->setProjectHint($meta->projectId)
            ->setDeviceName('php-sdk-live-conformance'),
        $meta,
    );
    expect($login?->getAccessToken())->not->toBe('');
    expect($login?->getRefreshToken())->not->toBe('');

    $auth = new UdbAuthClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000]);
    $auth->bindContext($meta);
    $auth->authenticateBearer($login->getAccessToken(), $meta);
    $refresh = $openAuthGenerated->refresh_token(
        (new RefreshTokenRequest())->setRefreshToken($login->getRefreshToken()),
        $meta,
    );
    expect($refresh?->getAccessToken())->not->toBe('');

    $authedMeta = liveMeta($login->getAccessToken());
    $data = new GeneratedClient(['endpoint' => $target, 'deadline_ms' => 2_000, 'retry' => ['max_attempts' => 1]]);
    $authGenerated = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 2_000, 'retry' => ['max_attempts' => 1]]);
    $data->bindContext($authedMeta);
    $authGenerated->bindContext($authedMeta);

    $capabilities = $data->get_capabilities(new CapabilitiesRequest(), $authedMeta);
    $enabled = array_map('strtolower', iterator_to_array($capabilities->getEnabledBackends()));
    $required = array_filter(array_map('trim', explode(',', liveEnv('UDB_LIVE_REQUIRED_BACKENDS', 'postgres,mongodb,minio'))));
    foreach ($required as $backend) {
        expect($enabled)->toContain(strtolower($backend));
    }

    $probed = 0;
    foreach (stubAccessors($data, $authGenerated) as $stubName => $stub) {
        $labelPrefix = "{$stubName}.";
        foreach (generatedStubMethods($stub) as $method) {
            $label = $labelPrefix.$method->getName();
            $params = $method->getParameters();
            $hasRequest = isset($params[0])
                && $params[0]->getType() instanceof ReflectionNamedType
                && ! $params[0]->getType()->isBuiltin();
        try {
            $call = $hasRequest
                ? $method->invoke($stub, requestFor($method), $authedMeta->toGrpcMetadata(), ['timeout' => 2_000_000])
                : $method->invoke($stub, $authedMeta->toGrpcMetadata(), ['timeout' => 2_000_000]);
            if (method_exists($call, 'responses')) {
                $responses = $call->responses();
                foreach ($responses as $_) {
                    break;
                }
                assertLiveStatusMounted($label, $call->getStatus());
            } elseif (method_exists($call, 'writesDone')) {
                $call->writesDone();
                if (method_exists($call, 'read')) {
                    $call->read();
                }
                if (method_exists($call, 'getStatus')) {
                    assertLiveStatusMounted($label, $call->getStatus());
                }
            } elseif (method_exists($call, 'wait')) {
                [, $status] = $call->wait();
                assertLiveStatusMounted($label, $status);
            }
        } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
            expect(isFatalLiveStatus($e->status))->toBeFalse("{$label} did not reach an implemented live RPC: {$e->getMessage()}");
        }
        $probed++;
        }
    }
    expect($probed)->toBe(262);
});
