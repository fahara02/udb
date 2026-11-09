<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Support;

use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Grpc\ChannelCredentials;

/**
 * Builds the gRPC channel credentials from the `tls` config block:
 *
 *   enabled     dial with TLS (implied by any of the three paths below)
 *   root_certs  PEM bundle to trust instead of the system roots
 *   cert_chain  PEM client certificate chain for mutual TLS (with private_key)
 *   private_key PEM private key of that client certificate (with cert_chain)
 *
 * A named file that is missing, unreadable or holds no PEM is refused, and so is a
 * client certificate without its key (or a key without its certificate): the
 * channel never falls back to the system roots or to no client certificate.
 */
final class TlsCredentials
{
    /**
     * Whether the `tls` block asks for TLS.
     *
     * @param  array<string,mixed>  $tls
     */
    public static function enabled(array $tls): bool
    {
        return (bool) ($tls['enabled'] ?? false)
            || self::path($tls, 'root_certs') !== null
            || self::path($tls, 'cert_chain') !== null
            || self::path($tls, 'private_key') !== null;
    }

    /**
     * Read and check the PEM files the `tls` block names.
     *
     * @param  array<string,mixed>  $tls
     * @return array{root_certs: ?string, cert_chain: ?string, private_key: ?string}
     */
    public static function pems(array $tls): array
    {
        $root = self::path($tls, 'root_certs');
        $chain = self::path($tls, 'cert_chain');
        $key = self::path($tls, 'private_key');
        if (($chain === null) !== ($key === null)) {
            throw new UdbConfigurationException(
                'UDB TLS: tls.cert_chain (UDB_TLS_CERT_FILE) and tls.private_key (UDB_TLS_KEY_FILE) '
                . 'must be set together (mutual TLS needs both the client certificate and its key)'
            );
        }

        return [
            'root_certs' => $root === null ? null : self::read('tls.root_certs (UDB_TLS_CA_FILE)', $root, '/-----BEGIN CERTIFICATE-----/'),
            'cert_chain' => $chain === null ? null : self::read('tls.cert_chain (UDB_TLS_CERT_FILE)', $chain, '/-----BEGIN CERTIFICATE-----/'),
            'private_key' => $key === null ? null : self::read('tls.private_key (UDB_TLS_KEY_FILE)', $key, '/-----BEGIN [A-Z ]*PRIVATE KEY-----/'),
        ];
    }

    /**
     * The channel credentials for the `tls` block: insecure when TLS is off.
     *
     * @param  array<string,mixed>  $tls
     */
    public static function channelCredentials(array $tls): ChannelCredentials
    {
        if (! self::enabled($tls)) {
            return ChannelCredentials::createInsecure();
        }
        $pems = self::pems($tls);

        return ChannelCredentials::createSsl($pems['root_certs'], $pems['private_key'], $pems['cert_chain']);
    }

    /** @param  array<string,mixed>  $tls */
    private static function path(array $tls, string $name): ?string
    {
        $value = $tls[$name] ?? null;
        if ($value === null) {
            return null;
        }
        $value = trim((string) $value);

        return $value === '' ? null : $value;
    }

    private static function read(string $label, string $path, string $marker): string
    {
        $data = is_readable($path) ? @file_get_contents($path) : false;
        if ($data === false) {
            throw new UdbConfigurationException("UDB TLS: {$label}: cannot read {$path}");
        }
        if (preg_match($marker, $data) !== 1) {
            throw new UdbConfigurationException("UDB TLS: {$label}: {$path} holds no PEM block of the expected kind");
        }

        return $data;
    }
}
