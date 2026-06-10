using Grpc.Core;
using StorageV1 = udb.core.Storage.Services.V1;

namespace Udb.Client;

/// <summary>
/// Ergonomic async facade over the native <c>StorageService</c>. Every wrapper
/// applies the shared <see cref="UdbProject"/> metadata headers and returns the
/// unwrapped response task. The raw generated client stays reachable via
/// <see cref="Raw"/> for RPCs / overloads not wrapped here.
/// </summary>
public sealed class UdbStorageClient
{
    private readonly StorageV1.StorageService.StorageServiceClient _client;
    private readonly Func<Metadata> _headers;

    internal UdbStorageClient(StorageV1.StorageService.StorageServiceClient client, Func<Metadata> headers)
    {
        _client = client;
        _headers = headers;
    }

    /// <summary>Raw generated storage service client.</summary>
    public StorageV1.StorageService.StorageServiceClient Raw => _client;

    /// <summary>Register a new upload and obtain a pre-signed upload URL.</summary>
    public Task<StorageV1.RegisterUploadResponse> RegisterUploadAsync(
        StorageV1.RegisterUploadRequest request, CancellationToken ct = default)
        => _client.RegisterUploadAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Finalize an upload after the object has been written to the store.</summary>
    public Task<StorageV1.FinalizeUploadResponse> FinalizeUploadAsync(
        StorageV1.FinalizeUploadRequest request, CancellationToken ct = default)
        => _client.FinalizeUploadAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Get a pre-signed download URL for a file.</summary>
    public Task<StorageV1.GetDownloadUrlResponse> GetDownloadUrlAsync(
        StorageV1.GetDownloadUrlRequest request, CancellationToken ct = default)
        => _client.GetDownloadUrlAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Get file metadata.</summary>
    public Task<StorageV1.GetFileResponse> GetFileAsync(
        StorageV1.GetFileRequest request, CancellationToken ct = default)
        => _client.GetFileAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Update file metadata.</summary>
    public Task<StorageV1.UpdateFileResponse> UpdateFileAsync(
        StorageV1.UpdateFileRequest request, CancellationToken ct = default)
        => _client.UpdateFileAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Delete a file (soft delete).</summary>
    public Task<StorageV1.DeleteFileResponse> DeleteFileAsync(
        StorageV1.DeleteFileRequest request, CancellationToken ct = default)
        => _client.DeleteFileAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>List files.</summary>
    public Task<StorageV1.ListFilesResponse> ListFilesAsync(
        StorageV1.ListFilesRequest request, CancellationToken ct = default)
        => _client.ListFilesAsync(request, _headers(), cancellationToken: ct).ResponseAsync;
}
