<?php

declare(strict_types=1);

/*
 * =============================================================================
 * UDB storage example: upload a file, then download it two ways.
 *
 * This shows the `UdbProject->storage()` facade over the generated
 * StorageService:
 *
 *   1. uploadFile()         RegisterUpload -> presigned PUT -> FinalizeUpload
 *   2. downloadFile()       presigned GetDownloadUrl (HAPPY PATH — bytes never
 *                           transit the broker; hand the URL to the client)
 *   3. downloadFileBytes()  server-streaming DownloadFile RPC (new in 0.3.6;
 *                           FALLBACK for clients with no egress to the store)
 *
 * Run against a live broker with an object store (e.g. MinIO/S3) configured:
 *
 *     UDB_ENDPOINT=127.0.0.1:50051 UDB_TENANT=t_acme \
 *         php examples/storage-download.php
 * =============================================================================
 */

require __DIR__ . '/../vendor/autoload.php';

use function Fahara02\UdbLaravel\createUdb;

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;

function env_or(string $key, string $default): string
{
    $v = getenv($key);

    return ($v === false || $v === '') ? $default : $v;
}

$udb = createUdb([
    'target'      => env_or('UDB_ENDPOINT', '127.0.0.1:50051'),
    'tenantId'    => env_or('UDB_TENANT', 'default'),
    'projectId'   => env_or('UDB_PROJECT', 'default'),
    'purpose'     => 'php.storage.example',
    'scopes'      => ['udb:read', 'udb:write'],
    'bearerToken' => env_or('UDB_BEARER_TOKEN', ''),
]);

$storage = $udb->storage();

$payload = "hello from the UDB PHP storage example\n";

try {
    // -------------------------------------------------------------------------
    // 1. Upload. One call does RegisterUpload -> presigned PUT -> FinalizeUpload.
    //    `uploadFile(filename, data, opts)` throws UdbRpcException if the broker
    //    returns no upload_url (storage degraded).
    // -------------------------------------------------------------------------
    $file = $storage->uploadFile('greeting.txt', $payload, [
        'contentType'   => 'text/plain',
        'fileType'      => 'example',
        'referenceType' => 'demo',
        'referenceId'   => 'php-storage-example',
    ]);
    $fileId = $file->getFileId();
    printf("uploaded: file_id=%s (%d bytes)\n", $fileId, strlen($payload));

    // -------------------------------------------------------------------------
    // 2. Download — HAPPY PATH. Exactly one GetDownloadUrl RPC; the typed
    //    GetDownloadUrlResponse carries a presigned URL the client fetches
    //    directly from the object store (bytes do NOT pass through the broker).
    // -------------------------------------------------------------------------
    $dl  = $storage->downloadFile($fileId, ['expiresInMinutes' => 15]);
    $url = $dl->getDownloadUrl();
    printf("presigned download url: %s\n", $url !== '' ? $url : '(none — broker returned empty)');

    // -------------------------------------------------------------------------
    // 3. Download — STREAMING FALLBACK (new in 0.3.6). Use only when the client
    //    cannot reach the object store over HTTP. downloadFileBytes() opens one
    //    server-streaming DownloadFile call and reassembles each
    //    DownloadFileChunk.data run in order.
    //
    //    BLOCKING: PHP ext-grpc has no async API, so this returns only after the
    //    final chunk lands (or the deadline trips) and buffers the body in
    //    memory. Prefer the presigned URL above for large objects.
    // -------------------------------------------------------------------------
    $bytes = $storage->downloadFileBytes($fileId);
    printf("streamed back %d bytes; round-trip %s\n", strlen($bytes), $bytes === $payload ? 'OK' : 'MISMATCH');

    // -------------------------------------------------------------------------
    // For RPCs without a wrapper, the generated stub is reachable directly. The
    // same server-stream done by hand: iterate the call and read each chunk.
    // (downloadFileBytes() above already does this for you.)
    //
    //     use Udb\Core\Storage\Services\V1\DownloadFileRequest;
    //     $req  = (new DownloadFileRequest())->setTenantId('t_acme')->setFileId($fileId);
    //     $call = $storage->client()->DownloadFile($req, $udb->metadata()->toGrpcMetadata());
    //     foreach ($call->responses() as $chunk) {
    //         $firstChunk = $chunk->getData(); // DownloadFileChunk.data
    //         break;                            // read the first chunk only
    //     }
    // -------------------------------------------------------------------------
} catch (UdbRpcException $e) {
    fprintf(STDERR, "storage RPC failed (status=%d): %s\n", $e->status, $e->getMessage());
    exit(1);
}
