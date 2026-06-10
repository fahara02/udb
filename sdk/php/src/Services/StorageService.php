<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Storage\Services\V1\DeleteFileRequest;
use Udb\Core\Storage\Services\V1\DeleteFileResponse;
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
        bool $isPublic = false,
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
            ->setIsPublic($isPublic)
            ->setExpiresInMinutes($expiresInMinutes)
            ->setSizeBytes($sizeBytes);

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
        bool $isPublic = false,
        int $sizeBytes = 0,
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
            ->setIsPublic($isPublic)
            ->setSizeBytes($sizeBytes);

        return $this->project->invoke(
            'FinalizeUpload',
            fn (array $md, array $o) => $this->stub->FinalizeUpload($request, $md, $o),
            $metadata,
            FinalizeUploadResponse::class,
        );
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
        bool $isPublic = false,
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
            ->setReferenceType($referenceType)
            ->setIsPublic($isPublic);

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
