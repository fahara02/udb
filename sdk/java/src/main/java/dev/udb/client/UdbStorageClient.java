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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;

/**
 * Blocking facade over the native {@code StorageService}. It rides the shared
 * control-plane channel and attaches the project {@link UdbMetadata} headers to
 * every call. The raw generated stub stays reachable via {@link #stub()}.
 */
public final class UdbStorageClient {
  @FunctionalInterface
  public interface HttpPut {
    void put(String url, byte[] data, String contentType);
  }

  public record UploadFileOptions(
      String contentType,
      String fileType,
      String referenceId,
      String referenceType,
      Boolean isPublic,
      int expiresInMinutes,
      String checksum,
      String etag) {
    public static UploadFileOptions defaults() {
      return new UploadFileOptions(
          "application/octet-stream", "", "", "", null, 0, "", "");
    }

    public UploadFileOptions {
      contentType =
          contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType;
      fileType = fileType == null ? "" : fileType;
      referenceId = referenceId == null ? "" : referenceId;
      referenceType = referenceType == null ? "" : referenceType;
      expiresInMinutes = Math.max(0, expiresInMinutes);
      checksum = checksum == null ? "" : checksum;
      etag = etag == null ? "" : etag;
    }
  }

  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private final StorageServiceGrpc.StorageServiceBlockingStub stub;
  private final UdbMetadataRef metadata;
  private final HttpPut httpPut;

  UdbStorageClient(Channel channel, UdbMetadata metadata) {
    this(channel, metadata, UdbCredentials.fromMetadata(metadata));
  }

  UdbStorageClient(Channel channel, UdbMetadata metadata, UdbCredentials credentials) {
    this(channel, new UdbMetadataRef(metadata), credentials, UdbStorageClient::defaultPut);
  }

  UdbStorageClient(Channel channel, UdbMetadataRef metadata, UdbCredentials credentials) {
    this(channel, metadata, credentials, UdbStorageClient::defaultPut);
  }

  UdbStorageClient(
      Channel channel, UdbMetadataRef metadata, UdbCredentials credentials, HttpPut httpPut) {
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.httpPut = Objects.requireNonNull(httpPut, "httpPut");
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

  public FinalizeUploadResponse uploadFile(String filename, byte[] data) {
    return uploadFile(filename, data, UploadFileOptions.defaults());
  }

  /**
   * Upload bytes through the broker-owned two-phase storage workflow:
   * RegisterUpload -> HTTP PUT to the presigned URL -> FinalizeUpload.
   *
   * <p>No proof Get/List is hidden here; an empty upload URL fails closed before
   * the PUT/finalize steps.
   */
  public FinalizeUploadResponse uploadFile(
      String filename, byte[] data, UploadFileOptions options) {
    Objects.requireNonNull(filename, "filename");
    Objects.requireNonNull(data, "data");
    UdbMetadata current = metadata.current();
    UploadFileOptions opts = options == null ? UploadFileOptions.defaults() : options;
    RegisterUploadRequest.Builder register =
        RegisterUploadRequest.newBuilder()
            .setTenantId(current.tenantId())
            .setProjectId(current.projectId())
            .setFilename(filename)
            .setContentType(opts.contentType())
            .setFileType(opts.fileType())
            .setReferenceId(opts.referenceId())
            .setReferenceType(opts.referenceType())
            .setExpiresInMinutes(opts.expiresInMinutes())
            .setSizeBytes(data.length);
    if (opts.isPublic() != null) {
      register.setIsPublic(opts.isPublic());
    }

    RegisterUploadResponse registered = registerUpload(register.build());
    if (registered.getUploadUrl().isBlank()) {
      throw new IllegalStateException("RegisterUpload returned no upload_url");
    }
    httpPut.put(registered.getUploadUrl(), data, opts.contentType());

    FinalizeUploadRequest.Builder finalize =
        FinalizeUploadRequest.newBuilder()
            .setTenantId(current.tenantId())
            .setFileId(registered.getFileId())
            .setContentType(opts.contentType())
            .setFileType(opts.fileType())
            .setReferenceId(opts.referenceId())
            .setReferenceType(opts.referenceType())
            .setSizeBytes(data.length);
    if (opts.isPublic() != null) {
      finalize.setIsPublic(opts.isPublic());
    }
    if (!opts.checksum().isBlank()) {
      finalize.setChecksum(opts.checksum());
    }
    if (!opts.etag().isBlank()) {
      finalize.setEtag(opts.etag());
    }
    return finalizeUpload(finalize.build());
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

  private static void defaultPut(String url, byte[] data, String contentType) {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(url))
            .header("content-type", contentType)
            .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
            .build();
    try {
      HttpResponse<Void> response = HTTP.send(request, HttpResponse.BodyHandlers.discarding());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new IllegalStateException("upload PUT failed with HTTP " + response.statusCode());
      }
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    } catch (InterruptedException err) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("upload PUT interrupted", err);
    }
  }
}
