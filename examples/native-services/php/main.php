<?php

declare(strict_types=1);

/**
 * Progressive, simplest→advanced tour of UDB's native auth services from PHP,
 * over the `UdbAuthClient` SDK wrapper (the consumer side):
 *
 *   1. Authenticate an API key → resolved principal (simplest).
 *   2. Authorize a resource/action — `can` (allowed vs. denied).
 *   3. Native DB fast-path grant — `nativeAccess` (advanced, Stage 2).
 *
 * Provisioning users/roles/keys is an admin concern — do it once with the Go
 * example (examples/native-services/go) and export the key as UDB_API_KEY.
 *
 * Prerequisites: the `grpc` PHP extension, a running UDB broker, and the SDK on
 * the autoloader (see README — use a Composer path repository pointing at
 * ../../../sdk/php). Run:  UDB_API_KEY=udbk_... php main.php
 */

require __DIR__ . '/vendor/autoload.php';

use Fahara02\UdbLaravel\UdbAuthClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Udb\Core\Authz\Services\V1\ResourceRef;

$endpoint = getenv('UDB_TARGET') ?: '127.0.0.1:50051';
$apiKey = getenv('UDB_API_KEY') ?: '';

$auth = new UdbAuthClient(['endpoint' => $endpoint]);
$auth->bindContext(new UdbMetadata(
    tenantId: 'acme',
    userId: '',
    purpose: 'control-plane',
    correlationId: 'native-php-example',
    scopes: ['udb:*'],
    serviceIdentity: 'examples.native-php',
    projectId: 'billing',
    clientCatalogVersion: '1.0.0',
));

// ── Step 1 (simplest): authenticate an API key → principal ───────────────────
if ($apiKey !== '') {
    $principal = $auth->authenticateApiKey($apiKey)->getPrincipal();
    printf("1) api key authenticated → user_id=%s\n", $principal?->getUserId() ?? '');
} else {
    echo "1) set UDB_API_KEY (mint one with the Go example) to see authentication\n";
}

// ── Step 2: authorize a resource/action ──────────────────────────────────────
$invoice = (new ResourceRef())->setResourceName('invoice')->setMessageType('invoice');
// `can` now forwards requested_scopes (explicit arg, else metadata scopes) and
// memoises the decision in an in-process, server-TTL'd cache.
[$allowed, $decision] = $auth->can($invoice, 'data.select', scopes: ['data.read']);
printf("2) can data.select on invoice → %s (decision_id=%s)\n", $allowed ? 'true' : 'false', $decision->getDecisionId());
[$denied] = $auth->can($invoice, 'data.delete');
printf("   can data.delete on invoice → %s\n", $denied ? 'true' : 'false');

// `explain` returns the decision without throwing; `require` throws
// UdbAuthzDeniedException on deny (ideal at the top of a controller action).
$exp = $auth->explain($invoice, 'data.select');
printf("   explain data.select → allowed=%s reason=%s\n", $exp->getAllowed() ? 'true' : 'false', $exp->getDenyReason());
try {
    $auth->require($invoice, 'data.delete', scopes: ['data.write']);
    echo "   require data.delete → allowed\n";
} catch (\Fahara02\UdbLaravel\Exceptions\UdbAuthzDeniedException $e) {
    printf("   require data.delete → denied: %s\n", $e->denyReason() ?: '(no reason)');
}

// `batchCan` checks many object/action pairs in one RPC.
$results = $auth->batchCan([
    ['object' => 'invoice', 'action' => 'data.select'],
    ['object' => 'invoice', 'action' => 'data.delete'],
]);
foreach ($results as $key => $ok) {
    printf("   batchCan %s → %s\n", $key, $ok ? 'true' : 'false');
}

// ── Step 3 (advanced): native DB fast-path grant ─────────────────────────────
try {
    $grant = $auth->nativeAccess($invoice, 'data.select');
    if ($grant === null) {
        echo "3) access allowed; no native grant minted (server native-access not configured)\n";
    } else {
        printf(
            "3) native grant: role=%s session_vars=%d (open PDO on \$grant->getDsn(), UdbAuthClient::withNativeTx)\n",
            $grant->getRole(),
            count($grant->getSessionVariables())
        );
    }
} catch (\Throwable $e) {
    printf("3) native access denied/unavailable: %s\n", $e->getMessage());
}

// ── Step 4: the unified UdbProject facade (one config, every service) ─────────
// `createUdb` builds a portable client exposing data()/auth()/authz()/apiKey()/
// tenant()/notification()/analytics() — sharing one metadata + channel.
$udb = \Fahara02\UdbLaravel\createUdb([
    'target' => $endpoint,
    'tenantId' => 'acme',
    'projectId' => 'billing',
    'purpose' => 'control-plane',
    'scopes' => ['udb:*'],
    'serviceIdentity' => 'examples.native-php',
]);
[$ok] = $udb->authz()->can($invoice, 'data.select');
printf("4) UdbProject facade: authz()->can data.select → %s\n", $ok ? 'true' : 'false');
echo "   services available: data, auth, authz, apiKey, tenant, notification, analytics\n";
// e.g. $udb->notification()->send('invoice.created', 'user-123');
//      $udb->apiKey()->create('ci-bot', ownerType: 1, ownerId: 'svc');
//      $udb->tenant()->onboard(code: 'acme', name: 'Acme Inc');
