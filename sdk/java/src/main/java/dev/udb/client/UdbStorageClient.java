package dev.udb.client;

import com.udb.core.storage.services.v1.DeleteFileRequest;
import com.udb.core.storage.services.v1.DeleteFileResponse;
import com.udb.core.storage.services.v1.FinalizeUploadRequest;
import com.udb.core.storage.services.v1.FinalizeUploadResponse;
import com.udb.core.storage.services.v1.GetDownloadUrlRequest;
import com.udb.core.storage.services.v1.GetDownloadUrlResponse;
import com.udb.core.storage.services.v1.GetFileRequest;
import com.udb.core.storage.services.v1.GetFileResponse;
import com.udb.core.storage.services.v1.ListFilesRequest;
import com.udb.core.storage.services.v1.ListFilesResponse;
import com.udb.core.storage.services.v1.RegisterUploadRequest;
import com.udb.core.storage.services.v1.RegisterUploadResponse;
import com.udb.core.storage.services.v1.StorageServiceGrpc;
import com.udb.core.storage.services.v1.UpdateFileRequest;
import com.udb.core.storage.services.v1.UpdateFileResponse;
import io.grpc.Channel;

/**
 * Blocking facade over the native {@code StorageService}. It rides the shared
 * control-plane channel and attaches the project {@link UdbMetadata} headers to
 * every call. The raw generated stub stays reachable via {@link #stub()}.
 */
public final class UdbStorageClient {
  private final StorageServiceGrpc.StorageServiceBlockingStub stub;

  UdbStorageClient(Channel channel, UdbMetadata metadata) {
    this(channel, metadata, UdbCredentials.fromMetadata(metadata));
  }

  UdbStorageClient(Channel channel, UdbMetadata metadata, UdbCredentials credentials) {
    this.stub =
        StorageServiceGrpc.newBlockingStub(channel)
            .withInterceptors(UdbClient.credentialInterceptor(metadata, credentials));
  }

  /** The raw generated blocking stub (never hidden). */
  public StorageServiceGrpc.StorageServiceBlockingStub stub() {
    return stub;
  }

  /** Reserve an upload slot and obtain a presigned PUT URL + file id. */
  public RegisterUploadResponse registerUpload(RegisterUploadRequest request) {
    return stub.registerUpload(request);
  }

  /** Confirm an upload completed; transitions the file out of PENDING. */
  public FinalizeUploadResponse finalizeUpload(FinalizeUploadRequest request) {
    return stub.finalizeUpload(request);
  }

  /** Mint a presigned GET URL for downloading a stored object. */
  public GetDownloadUrlResponse getDownloadUrl(GetDownloadUrlRequest request) {
    return stub.getDownloadUrl(request);
  }

  /** Fetch a file's metadata by id. */
  public GetFileResponse getFile(GetFileRequest request) {
    return stub.getFile(request);
  }

  /** Update mutable file metadata (name, tags, etc.). */
  public UpdateFileResponse updateFile(UpdateFileRequest request) {
    return stub.updateFile(request);
  }

  /** Delete a file (and schedule object GC). */
  public DeleteFileResponse deleteFile(DeleteFileRequest request) {
    return stub.deleteFile(request);
  }

  /** List files for the tenant, with paging/filter on the request. */
  public ListFilesResponse listFiles(ListFilesRequest request) {
    return stub.listFiles(request);
  }
}
