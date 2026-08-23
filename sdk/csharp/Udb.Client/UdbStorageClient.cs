using Grpc.Core;
using StorageV1 = Udb.Core.Storage.Services.V1;

namespace Udb.Client;

/// <summary>
/// Ergonomic async facade over the native <c>StorageService</c>. Every wrapper
/// applies the shared <see cref="UdbProject"/> metadata headers and returns the
/// unwrapped response task. The raw generated client stays reachable via
/// <see cref="Raw"/> for RPCs / overloads not wrapped here.
/// </summary>
public sealed class UdbStorageClient
{
    public sealed record UploadFileOptions(
        string ContentType = "application/octet-stream",
        string FileType = "",
        string ReferenceId = "",
        string ReferenceType = "",
        bool? IsPublic = null,
        int ExpiresInMinutes = 0,
        string Checksum = "",
        string Etag = "");

    private static readonly HttpClient DefaultHttpClient = new();

    private readonly StorageV1.StorageService.StorageServiceClient _client;
    private readonly Func<Metadata> _headers;
    private readonly Func<string, byte[], string, CancellationToken, Task> _putBytes;

    internal UdbStorageClient(
        StorageV1.StorageService.StorageServiceClient client,
        Func<Metadata> headers,
        Func<string, byte[], string, CancellationToken, Task>? putBytes = null)
    {
        _client = client;
        _headers = headers;
        _putBytes = putBytes ?? PutBytesAsync;
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

    /// <summary>
    /// Upload bytes through the storage two-phase workflow:
    /// RegisterUpload -> HTTP PUT to the presigned URL -> FinalizeUpload. No
    /// hidden Get/List proof calls are issued.
    /// </summary>
    public async Task<StorageV1.FinalizeUploadResponse> UploadFileAsync(
        string filename,
        byte[] data,
        UploadFileOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(filename);
        ArgumentNullException.ThrowIfNull(data);
        var opts = options ?? new UploadFileOptions();
        var contentType = string.IsNullOrWhiteSpace(opts.ContentType)
            ? "application/octet-stream"
            : opts.ContentType;
        var headers = _headers();
        var register = new StorageV1.RegisterUploadRequest
        {
            TenantId = Header(headers, "x-tenant-id"),
            ProjectId = Header(headers, "x-udb-project-id"),
            Filename = filename,
            ContentType = contentType,
            FileType = opts.FileType,
            ReferenceId = opts.ReferenceId,
            ReferenceType = opts.ReferenceType,
            ExpiresInMinutes = Math.Max(0, opts.ExpiresInMinutes),
            SizeBytes = data.LongLength,
        };
        if (opts.IsPublic.HasValue)
        {
            register.IsPublic = opts.IsPublic.Value;
        }

        var registered = await _client.RegisterUploadAsync(register, headers, cancellationToken: ct).ResponseAsync
            .ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(registered.UploadUrl))
        {
            throw new InvalidOperationException("RegisterUpload returned no upload_url");
        }

        await _putBytes(registered.UploadUrl, data, contentType, ct).ConfigureAwait(false);

        var finalize = new StorageV1.FinalizeUploadRequest
        {
            TenantId = Header(headers, "x-tenant-id"),
            FileId = registered.FileId,
            ContentType = contentType,
            FileType = opts.FileType,
            ReferenceId = opts.ReferenceId,
            ReferenceType = opts.ReferenceType,
            SizeBytes = data.LongLength,
        };
        if (opts.IsPublic.HasValue)
        {
            finalize.IsPublic = opts.IsPublic.Value;
        }
        if (!string.IsNullOrEmpty(opts.Checksum))
        {
            finalize.Checksum = opts.Checksum;
        }
        if (!string.IsNullOrEmpty(opts.Etag))
        {
            finalize.Etag = opts.Etag;
        }

        return await _client.FinalizeUploadAsync(finalize, headers, cancellationToken: ct).ResponseAsync
            .ConfigureAwait(false);
    }

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

    private static string Header(Metadata headers, string name)
        => headers.GetValue(name) ?? string.Empty;

    private static async Task PutBytesAsync(
        string url,
        byte[] data,
        string contentType,
        CancellationToken ct)
    {
        using var content = new ByteArrayContent(data);
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(contentType);
        using var response = await DefaultHttpClient.PutAsync(url, content, ct).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }
}
