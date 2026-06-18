<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Storage\Services\V1\DeleteFileRequest;
use Udb\Core\Storage\Services\V1\DeleteFileResponse;
use Udb\Core\Storage\Services\V1\DownloadFileRequest;
use Udb\Core\Storage\Services\V1\FinalizeUploadRequest;
use Udb\Core\Storage\Services\V1\FinalizeUploadResponse;
use Udb\Core\Storage\Services\V1\GetDownloadUrlRequest;
use Udb\Core\Storage\Services\V1\GetDownloadUrlResponse;
use Udb\Core\Storage\Services\V1\GetFileRequest;
use Udb\Core\Storage\Services\V1\GetFileResponse;
use Udb\Core\Storage\Services\V1\ListFilesRequest;
use Udb\Core\Storage\Services\V1\ListFilesResponse;
use Udb\Core\Storage\Services\V1\RegisterUploadRequest;
use Udb\Core\Storage\Services\V1\RegisterUploadResponse;
use Udb\Core\Storage\Services\V1\StorageServiceClient;
use Udb\Core\Storage\Services\V1\UpdateFileRequest;
use Udb\Core\Storage\Services\V1\UpdateFileResponse;

/**
 * Convenience wrapper over the generated {@see StorageServiceClient}.
 * Shares the project's metadata + deadline; the raw stub remains reachable
 * via {@see client()} for RPCs not wrapped here.
 *
 * The two-phase upload flow is: {@see registerUpload()} returns a pre-signed
 * PUT URL the caller uses to write the object to the store directly, then
 * {@see finalizeUpload()} marks the file durable once the bytes have landed.
 */
final class StorageService
{
    /**
     * Test-only seam: override how the `DownloadFile` server-stream is opened.
     * The opener receives the built {@see DownloadFileRequest} + the gRPC
     * metadata headers and must return a stream object exposing `responses()`
     * (and optionally `getStatus()`) — the `\Grpc\ServerStreamingCall` contract.
     * Production leaves this null and uses the generated stub.
     *
     * @var null|callable(DownloadFileRequest, array<string,list<string>>):object
     */
    private $downloadStreamOpener = null;

    public function __construct(
        private readonly UdbProject $project,
        private readonly StorageServiceClient $stub,
    ) {
    }

    /** The raw generated stub, for RPCs without a convenience wrapper. */
    public function client(): StorageServiceClient
    {
        return $this->stub;
    }

    /**
     * Override how {@see downloadFileBytes()} opens its `DownloadFile`
     * server-stream. Test-only seam so the streaming reassembly can be asserted
     * offline (no ext-grpc / object store). Returns `$this` for chaining.
     *
     * @param  callable(DownloadFileRequest, array<string,list<string>>):object  $opener
     */
    public function withDownloadStreamOpener(callable $opener): self
    {
        $this->downloadStreamOpener = $opener;

        return $this;
    }

    /**
     * Register a new upload and obtain a pre-signed upload URL. `$fileType`,
     * `$referenceType` and `$referenceId` are the broker's classification +
     * back-reference fields (e.g. file_type="avatar", reference_type="user").
     */
    public function registerUpload(
        string $filename,
        string $contentType,
        string $fileType = '',
        string $referenceId = '',
        string $referenceType = '',
        ?bool $isPublic = null,
        int $expiresInMinutes = 0,
        int $sizeBytes = 0,
        ?UdbMetadata $metadata = null,
    ): RegisterUploadResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new RegisterUploadRequest())
            ->setTenantId($meta->tenantId)
            ->setProjectId($meta->projectId)
            ->setFilename($filename)
            ->setContentType($contentType)
            ->setFileType($fileType)
            ->setReferenceId($referenceId)
            ->setReferenceType($referenceType)
            ->setExpiresInMinutes($expiresInMinutes)
            ->setSizeBytes($sizeBytes);
        // `is_public` is a proto3 optional bool — only set it when supplied so an
        // omitted value stays UNSET on the wire (proto3 explicit presence).
        if ($isPublic !== null) {
            $request->setIsPublic($isPublic);
        }

        return $this->project->invoke(
            'RegisterUpload',
            fn (array $md, array $o) => $this->stub->RegisterUpload($request, $md, $o),
            $metadata,
            RegisterUploadResponse::class,
        );
    }

    /**
     * Finalize an upload after the object has been written to the store.
     */
    public function finalizeUpload(
        string $fileId,
        string $contentType = '',
        string $fileType = '',
        string $referenceId = '',
        string $referenceType = '',
        ?bool $isPublic = null,
        int $sizeBytes = 0,
        string $checksum = '',
        string $etag = '',
        ?UdbMetadata $metadata = null,
    ): FinalizeUploadResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new FinalizeUploadRequest())
            ->setTenantId($meta->tenantId)
            ->setFileId($fileId)
            ->setContentType($contentType)
            ->setFileType($fileType)
            ->setReferenceId($referenceId)
            ->setReferenceType($referenceType)
            ->setSizeBytes($sizeBytes);
        if ($isPublic !== null) {
            $request->setIsPublic($isPublic);
        }
        if ($checksum !== '') {
            $request->setChecksum($checksum);
        }
        if ($etag !== '') {
            $request->setEtag($etag);
        }

        return $this->project->invoke(
            'FinalizeUpload',
            fn (array $md, array $o) => $this->stub->FinalizeUpload($request, $md, $o),
            $metadata,
            FinalizeUploadResponse::class,
        );
    }

    /**
     * One-call upload: RegisterUpload → HTTP PUT bytes → FinalizeUpload.
     *
     * Canonical signature `uploadFile(filename, data, opts)` (D13, parity with
     * Go/TS/Python). `$opts` carries `contentType`/`isPublic`/`fileType`/
     * `referenceId`/`referenceType`/`expiresInMinutes`. Exactly three honest ops
     * with NO proof Get/List. Throws {@see UdbRpcException} when the broker
     * returns no `upload_url` (degraded mode, surfaced from the typed
     * `RegisterUploadResponse.error`).
     *
     * `$httpPut` is an injectable PUT (`fn(string $url, string $data, string $contentType): void`)
     * so tests can assert the sequence without real network I/O; the default
     * uses curl.
     *
     * @param  array{contentType?:string, isPublic?:?bool, fileType?:string, referenceId?:string, referenceType?:string, expiresInMinutes?:int}  $opts
     */
    public function uploadFile(
        string $filename,
        string $data,
        array $opts = [],
        ?UdbMetadata $metadata = null,
        ?callable $httpPut = null,
    ): FinalizeUploadResponse {
        $contentType = (string) ($opts['contentType'] ?? '') ?: 'application/octet-stream';
        $fileType = (string) ($opts['fileType'] ?? '');
        $referenceId = (string) ($opts['referenceId'] ?? '');
        $referenceType = (string) ($opts['referenceType'] ?? '');
        $isPublic = array_key_exists('isPublic', $opts) ? $opts['isPublic'] : null;
        $expiresInMinutes = (int) ($opts['expiresInMinutes'] ?? 0);

        $register = $this->registerUpload(
            $filename,
            $contentType,
            $fileType,
            $referenceId,
            $referenceType,
            $isPublic,
            $expiresInMinutes,
            strlen($data),
            $metadata,
        );
        $url = $register->getUploadUrl();
        if ($url === '') {
            $err = $register->getError();
            $msg = ($err !== null && method_exists($err, 'getMessage') && $err->getMessage() !== '')
                ? $err->getMessage()
                : 'RegisterUpload returned no upload_url (storage degraded)';
            throw new UdbRpcException(status: 9, details: "uploadFile: {$msg}", rpcName: 'RegisterUpload');
        }
        $put = $httpPut ?? self::defaultHttpPut(...);
        $put($url, $data, $contentType);

        return $this->finalizeUpload(
            $register->getFileId(),
            $contentType,
            $fileType,
            $referenceId,
            $referenceType,
            $isPublic,
            strlen($data),
            '',
            '',
            $metadata,
        );
    }

    /** Default presigned-URL PUT via curl (no extra dependency). */
    private static function defaultHttpPut(string $url, string $data, string $contentType): void
    {
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_CUSTOMREQUEST => 'PUT',
            CURLOPT_POSTFIELDS => $data,
            CURLOPT_HTTPHEADER => ['Content-Type: '.$contentType],
            CURLOPT_RETURNTRANSFER => true,
        ]);
        $ok = curl_exec($ch);
        $status = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        $error = curl_error($ch);
        curl_close($ch);
        if ($ok === false || $status >= 400) {
            throw new UdbRpcException(
                status: 13,
                details: "uploadFile: PUT failed (http={$status}) {$error}",
                rpcName: 'PUT',
            );
        }
    }

    /** Get a pre-signed download URL for a file. */
    public function getDownloadUrl(
        string $fileId,
        int $expiresInMinutes = 0,
        ?UdbMetadata $metadata = null,
    ): GetDownloadUrlResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new GetDownloadUrlRequest())
            ->setTenantId($meta->tenantId)
            ->setFileId($fileId)
            ->setExpiresInMinutes($expiresInMinutes);

        return $this->project->invoke(
            'GetDownloadUrl',
            fn (array $md, array $o) => $this->stub->GetDownloadUrl($request, $md, $o),
            $metadata,
            GetDownloadUrlResponse::class,
        );
    }

    /**
     * Canonical naming-contract alias of {@see getDownloadUrl()}
     * (`storage->downloadFile(fileId, opts?)`, parity with TS
     * `storage.downloadFile` / Python `download_file` / Go `DownloadFile`).
     *
     * PREFERS the presigned URL: emits EXACTLY one `GetDownloadUrl` RPC and
     * returns the typed {@see GetDownloadUrlResponse} — no hidden Get/List, no
     * bytes pulled through the broker. This is the unchanged default behavior.
     * When the caller actually needs the bytes and cannot reach the object store
     * over HTTP (no egress, corporate proxy, …), use {@see downloadFileBytes()},
     * which falls back to the server-streaming `DownloadFile` RPC.
     *
     * `$opts` carries `expiresInMinutes`. `getDownloadUrl()` stays as the prior
     * name.
     *
     * @param  array{expiresInMinutes?:int}  $opts
     */
    public function downloadFile(
        string $fileId,
        array $opts = [],
        ?UdbMetadata $metadata = null,
    ): GetDownloadUrlResponse {
        return $this->getDownloadUrl(
            $fileId,
            (int) ($opts['expiresInMinutes'] ?? 0),
            $metadata,
        );
    }

    /**
     * STREAMING fallback download: pull a file's raw bytes directly through the
     * broker via the server-streaming `DownloadFile` RPC, reassembling the
     * `DownloadFileChunk.data` byte-runs in order. Use this only when the
     * presigned `GetDownloadUrl` path ({@see downloadFile()}) is not usable
     * (e.g. the client has no egress to the object store); it is more expensive
     * because every byte transits the broker.
     *
     * Emits EXACTLY one `DownloadFile` RPC (one server-stream) — no Get/List.
     * `$chunkSizeBytes` is an advisory hint to the server (0 = server default).
     *
     * SYNC LIMITATION: PHP's ext-grpc has no async API, so iterating
     * `$call->responses()` is a BLOCKING read — this call does not return until
     * the broker has streamed the final chunk (or the deadline trips). For a
     * very large object this blocks the calling worker for the whole transfer
     * and buffers the full result in memory; prefer the presigned URL for big
     * files. The broker itself never buffers the whole object (it streams in
     * bounded chunks); the buffering here is purely client-side reassembly.
     *
     * @return string the reassembled file bytes
     */
    public function downloadFileBytes(
        string $fileId,
        int $chunkSizeBytes = 0,
        ?UdbMetadata $metadata = null,
    ): string {
        $meta = $this->project->metadata($metadata);
        $request = (new DownloadFileRequest())
            ->setTenantId($meta->tenantId)
            ->setFileId($fileId);
        if ($chunkSizeBytes > 0) {
            $request->setChunkSizeBytes($chunkSizeBytes);
        }
        $headers = $meta->toGrpcMetadata();

        $opener = $this->downloadStreamOpener
            ?? fn (DownloadFileRequest $req, array $md): object => $this->stub->DownloadFile($req, $md);
        $call = $opener($request, $headers);

        $bytes = '';
        foreach ($call->responses() as $chunk) {
            // DownloadFileChunk.data carries the next run of object bytes.
            if (is_object($chunk) && method_exists($chunk, 'getData')) {
                $bytes .= (string) $chunk->getData();
            }
        }

        // A non-OK trailing status means the stream faulted mid-transfer; surface
        // it as a typed error rather than silently returning a truncated body.
        if (method_exists($call, 'getStatus')) {
            $status = $call->getStatus();
            $code = is_object($status) ? (int) ($status->code ?? 0) : (int) ($status['code'] ?? 0);
            if ($code !== 0) {
                throw UdbRpcException::fromGrpcStatus($status, 'DownloadFile');
            }
        }

        return $bytes;
    }

    /** Get file metadata. */
    public function getFile(string $fileId, ?UdbMetadata $metadata = null): GetFileResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new GetFileRequest())
            ->setTenantId($meta->tenantId)
            ->setFileId($fileId);

        return $this->project->invoke(
            'GetFile',
            fn (array $md, array $o) => $this->stub->GetFile($request, $md, $o),
            $metadata,
            GetFileResponse::class,
        );
    }

    /** Update file metadata. */
    public function updateFile(
        string $fileId,
        string $filename = '',
        string $contentType = '',
        string $fileType = '',
        string $referenceId = '',
        string $referenceType = '',
        ?bool $isPublic = null,
        ?UdbMetadata $metadata = null,
    ): UpdateFileResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new UpdateFileRequest())
            ->setTenantId($meta->tenantId)
            ->setFileId($fileId)
            ->setFilename($filename)
            ->setContentType($contentType)
            ->setFileType($fileType)
            ->setReferenceId($referenceId)
            ->setReferenceType($referenceType);
        if ($isPublic !== null) {
            $request->setIsPublic($isPublic);
        }

        return $this->project->invoke(
            'UpdateFile',
            fn (array $md, array $o) => $this->stub->UpdateFile($request, $md, $o),
            $metadata,
            UpdateFileResponse::class,
        );
    }

    /** Delete a file (soft delete). */
    public function deleteFile(string $fileId, ?UdbMetadata $metadata = null): DeleteFileResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new DeleteFileRequest())
            ->setTenantId($meta->tenantId)
            ->setFileId($fileId);

        return $this->project->invoke(
            'DeleteFile',
            fn (array $md, array $o) => $this->stub->DeleteFile($request, $md, $o),
            $metadata,
            DeleteFileResponse::class,
        );
    }

    /**
     * List files, optionally filtered by type / reference / uploader.
     */
    public function listFiles(
        string $fileType = '',
        string $referenceId = '',
        string $referenceType = '',
        string $uploadedBy = '',
        int $page = 0,
        int $pageSize = 0,
        ?UdbMetadata $metadata = null,
    ): ListFilesResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new ListFilesRequest())
            ->setTenantId($meta->tenantId)
            ->setFileType($fileType)
            ->setReferenceId($referenceId)
            ->setReferenceType($referenceType)
            ->setUploadedBy($uploadedBy)
            ->setPage($page)
            ->setPageSize($pageSize);

        return $this->project->invoke(
            'ListFiles',
            fn (array $md, array $o) => $this->stub->ListFiles($request, $md, $o),
            $metadata,
            ListFilesResponse::class,
        );
    }
}
