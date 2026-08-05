<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Storage\Services\V1;

/**
 */
class StorageServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Register a new upload and obtain a pre-signed upload URL
     * @param \Udb\Core\Storage\Services\V1\RegisterUploadRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Storage\Services\V1\RegisterUploadResponse>
     */
    public function RegisterUpload(\Udb\Core\Storage\Services\V1\RegisterUploadRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.storage.services.v1.StorageService/RegisterUpload',
        $argument,
        ['\Udb\Core\Storage\Services\V1\RegisterUploadResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Finalize an upload after the object has been written to the store
     * @param \Udb\Core\Storage\Services\V1\FinalizeUploadRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Storage\Services\V1\FinalizeUploadResponse>
     */
    public function FinalizeUpload(\Udb\Core\Storage\Services\V1\FinalizeUploadRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.storage.services.v1.StorageService/FinalizeUpload',
        $argument,
        ['\Udb\Core\Storage\Services\V1\FinalizeUploadResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a pre-signed download URL for a file
     * @param \Udb\Core\Storage\Services\V1\GetDownloadUrlRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Storage\Services\V1\GetDownloadUrlResponse>
     */
    public function GetDownloadUrl(\Udb\Core\Storage\Services\V1\GetDownloadUrlRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.storage.services.v1.StorageService/GetDownloadUrl',
        $argument,
        ['\Udb\Core\Storage\Services\V1\GetDownloadUrlResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Reissue a presigned PUT URL for an existing PENDING upload — the resume path
     * when a RegisterUpload response was lost in flight (the client kept the file_id
     * but not the secret upload URL). The File row + object_key are unchanged; only
     * a fresh short-lived upload URL is minted. Rejected fail-closed for a
     * non-PENDING (already-finalized or removed) file. READ-ONLY (no state change).
     * @param \Udb\Core\Storage\Services\V1\ReissueUploadUrlRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Storage\Services\V1\ReissueUploadUrlResponse>
     */
    public function ReissueUploadUrl(\Udb\Core\Storage\Services\V1\ReissueUploadUrlRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.storage.services.v1.StorageService/ReissueUploadUrl',
        $argument,
        ['\Udb\Core\Storage\Services\V1\ReissueUploadUrlResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Stream a file's bytes directly through the broker. FALLBACK for clients
     * that cannot use the presigned `GetDownloadUrl` HTTP GET (no egress to the
     * object store, corporate proxy, etc.). The broker streams the object bytes
     * in bounded chunks server-side; it never buffers the whole object.
     * @param \Udb\Core\Storage\Services\V1\DownloadFileRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function DownloadFile(\Udb\Core\Storage\Services\V1\DownloadFileRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/udb.core.storage.services.v1.StorageService/DownloadFile',
        $argument,
        ['\Udb\Core\Storage\Services\V1\DownloadFileChunk', 'decode'],
        $metadata, $options);
    }

    /**
     * Get file metadata
     * @param \Udb\Core\Storage\Services\V1\GetFileRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Storage\Services\V1\GetFileResponse>
     */
    public function GetFile(\Udb\Core\Storage\Services\V1\GetFileRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.storage.services.v1.StorageService/GetFile',
        $argument,
        ['\Udb\Core\Storage\Services\V1\GetFileResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Update file metadata
     * @param \Udb\Core\Storage\Services\V1\UpdateFileRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Storage\Services\V1\UpdateFileResponse>
     */
    public function UpdateFile(\Udb\Core\Storage\Services\V1\UpdateFileRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.storage.services.v1.StorageService/UpdateFile',
        $argument,
        ['\Udb\Core\Storage\Services\V1\UpdateFileResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete a file. `mode` selects soft-delete (default, metadata tombstone +
     * best-effort byte removal) or hard-delete (durable object-GC intent committed
     * atomically with the tombstone, then driven to convergence — a byte-delete
     * failure returns an error, never success, and leaves the intent for the sweep).
     * @param \Udb\Core\Storage\Services\V1\DeleteFileRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Storage\Services\V1\DeleteFileResponse>
     */
    public function DeleteFile(\Udb\Core\Storage\Services\V1\DeleteFileRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.storage.services.v1.StorageService/DeleteFile',
        $argument,
        ['\Udb\Core\Storage\Services\V1\DeleteFileResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List files
     * @param \Udb\Core\Storage\Services\V1\ListFilesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Storage\Services\V1\ListFilesResponse>
     */
    public function ListFiles(\Udb\Core\Storage\Services\V1\ListFilesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.storage.services.v1.StorageService/ListFiles',
        $argument,
        ['\Udb\Core\Storage\Services\V1\ListFilesResponse', 'decode'],
        $metadata, $options);
    }

}
