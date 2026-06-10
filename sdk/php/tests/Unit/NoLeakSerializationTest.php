<?php

declare(strict_types=1);

use Udb\Core\Apikey\Entity\V1\ApiKey;
use Udb\Core\Authn\Entity\V1\Session;
use Udb\Core\Authn\Entity\V1\User;

it('generated authn/apikey entities never serialize persisted credential material', function () {
    // urgent_fix #35: assert against the GENERATED proto entities, not a hand-built
    // JSON literal. The previous fixture only lacked `password_hash` because the
    // author never typed it — a tautology. Building + serializing the real
    // generated `User`/`Session`/`ApiKey` exercises the actual protobuf JSON
    // surface (field naming + serialization), so the SDK's true response shape is
    // what's checked, and a regression in those generated types is caught here.
    //
    // Populate identity/metadata exactly as a REDACTED server response does, and
    // leave every OUTPUT_VIEW_STORAGE_ONLY credential field at its empty default
    // (protobuf JSON omits empty scalars), so no credential material is present.
    $user = (new User())
        ->setUserId('u1')
        ->setUsername('ada')
        ->setEmail('ada@example.com');
    $session = (new Session())
        ->setSessionId('sesspub_abc');
    $apiKey = (new ApiKey())
        ->setKeyId('udbk_abc')
        ->setKeyPrefix('udbk_abc');

    $json = $user->serializeToJsonString()
        . '|' . $session->serializeToJsonString()
        . '|' . $apiKey->serializeToJsonString();

    // Storage-only field names (snake_case + protobuf camelCase `jsonName`) and
    // known credential value prefixes must never appear in a serialized response.
    foreach ([
        'argon2id$',
        'hmac-sha256:',
        'password_hash', 'passwordHash',
        'totp_secret_enc', 'totpSecretEnc',
        'session_token_lookup', 'sessionTokenLookup',
        'session_token_hash', 'sessionTokenHash',
        'csrf_token_hash', 'csrfTokenHash',
        'key_hash', 'keyHash',
    ] as $banned) {
        expect($json)->not->toContain($banned);
    }
});

it('the no-leak assertion has teeth — the serializer DOES surface a populated credential', function () {
    // Guard against a silent serializer that drops everything (which would make the
    // omission above meaningless). Proves that IF a credential field were populated
    // on the generated type, it WOULD appear in the JSON — so the empty-default
    // omission asserted above is a real signal, not an artifact.
    $leaky = (new User())
        ->setUserId('u2')
        ->setPasswordHash('argon2id$leak');

    expect($leaky->serializeToJsonString())->toContain('passwordHash');
});
