<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Fahara02\UdbLaravel\Support\TlsCredentials;

/*
 * TlsCredentials reads the `tls` block's PEM files and refuses a bad one. The
 * file checks are pure PHP; building the ChannelCredentials needs ext-grpc and
 * is not exercised here.
 */

function tlsPemBlock(string $label): string
{
    // Structurally PEM; the helper checks the blocks, gRPC parses them on dial.
    return "-----BEGIN {$label}-----\nAAAA\n-----END {$label}-----\n";
}

function tlsFixtures(): array
{
    $dir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'udb-tls-' . bin2hex(random_bytes(4));
    mkdir($dir);
    file_put_contents("{$dir}/client.pem", tlsPemBlock('CERTIFICATE'));
    file_put_contents("{$dir}/client.key", tlsPemBlock('PRIVATE KEY'));
    file_put_contents("{$dir}/junk.pem", 'not a certificate');

    return [
        'cert' => "{$dir}/client.pem",
        'key' => "{$dir}/client.key",
        'junk' => "{$dir}/junk.pem",
        'missing' => "{$dir}/absent.pem",
    ];
}

it('reads the CA and the client pair, and any path turns TLS on', function () {
    $f = tlsFixtures();
    $tls = ['root_certs' => $f['cert'], 'cert_chain' => $f['cert'], 'private_key' => $f['key']];
    expect(TlsCredentials::enabled($tls))->toBeTrue();
    expect(TlsCredentials::pems($tls))->toBe([
        'root_certs' => tlsPemBlock('CERTIFICATE'),
        'cert_chain' => tlsPemBlock('CERTIFICATE'),
        'private_key' => tlsPemBlock('PRIVATE KEY'),
    ]);
    expect(TlsCredentials::enabled(['root_certs' => $f['cert']]))->toBeTrue();
    expect(TlsCredentials::enabled(['enabled' => false, 'root_certs' => '', 'cert_chain' => null]))->toBeFalse();
    expect(TlsCredentials::pems(['enabled' => true]))->toBe([
        'root_certs' => null,
        'cert_chain' => null,
        'private_key' => null,
    ]);
});

it('refuses half a client pair, naming both settings', function (string $present) {
    $f = tlsFixtures();
    $tls = $present === 'cert' ? ['cert_chain' => $f['cert']] : ['private_key' => $f['key']];
    expect(fn () => TlsCredentials::pems($tls))
        ->toThrow(UdbConfigurationException::class, 'tls.cert_chain (UDB_TLS_CERT_FILE) and tls.private_key (UDB_TLS_KEY_FILE)');
})->with(['cert', 'key']);

it('refuses a missing or non-PEM file, naming the setting and path', function (array $tls, string $setting, string $file) {
    $f = tlsFixtures();
    $tls = array_map(fn (string $name) => $f[$name], $tls);
    try {
        TlsCredentials::pems($tls);
        test()->fail('expected UdbConfigurationException');
    } catch (UdbConfigurationException $e) {
        expect($e->getMessage())->toContain($setting)->toContain($f[$file]);
    }
})->with([
    'missing CA' => [['root_certs' => 'missing'], 'UDB_TLS_CA_FILE', 'missing'],
    'CA without PEM' => [['root_certs' => 'junk'], 'UDB_TLS_CA_FILE', 'junk'],
    'missing key' => [['cert_chain' => 'cert', 'private_key' => 'missing'], 'UDB_TLS_KEY_FILE', 'missing'],
    'certificate as key' => [['cert_chain' => 'cert', 'private_key' => 'cert'], 'UDB_TLS_KEY_FILE', 'cert'],
]);
