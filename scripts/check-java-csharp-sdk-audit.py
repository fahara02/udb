#!/usr/bin/env python3
"""Source guard for the Chapter 10 Java/C# parity audit notes.

This guard pins the Java/C# SDK parity helpers that replaced the old helper-gap
notes: login-and-adopt, uploadFile, bound entity/table CRUD handles, typed
WriteReceipt/ReadFence, one-shot metadata read-fence helpers, and decoded
error-detail convenience fields.
"""

from __future__ import annotations

import argparse
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class SourceCheck:
    label: str
    path: str
    required: tuple[str, ...]
    forbidden: tuple[str, ...] = ()


CHECKS: tuple[SourceCheck, ...] = (
    SourceCheck(
        "java storage uploadFile helper",
        "sdk/java/src/main/java/dev/udb/client/UdbStorageClient.java",
        (
            "public RegisterUploadResponse registerUpload(",
            "public FinalizeUploadResponse finalizeUpload(",
            "public GetDownloadUrlResponse getDownloadUrl(",
            "public FinalizeUploadResponse uploadFile(",
            "RegisterUpload -> HTTP PUT to the presigned URL -> FinalizeUpload",
            "RegisterUploadRequest.newBuilder()",
            "registered.getUploadUrl().isBlank()",
            "httpPut.put(registered.getUploadUrl(), data, opts.contentType())",
            "FinalizeUploadRequest.newBuilder()",
            "defaultPut(String url, byte[] data, String contentType)",
        ),
    ),
    SourceCheck(
        "java project login-and-adopt workflow",
        "sdk/java/src/main/java/dev/udb/client/UdbProject.java",
        (
            "public UdbAuthClient auth()",
            "public UdbStorageClient storage()",
            "public void setCredentials(",
            "public AuthnResponse loginAndAdoptTenant(",
            "LoginResponse login = auth.login(request)",
            "AuthnResponse verified = auth.authenticateBearer(token)",
            "Principal principal = verified.getPrincipal()",
            "metadata.set(adopted)",
            "credentials.setBearerToken(token)",
        ),
    ),
    SourceCheck(
        "java auth exposes Login wrapper",
        "sdk/java/src/main/java/dev/udb/client/UdbAuthClient.java",
        (
            "public LoginResponse login(LoginRequest request)",
            ".login(request)",
            "metadata.current()",
        ),
    ),
    SourceCheck(
        "java data client exposes bound entity/table helpers",
        "sdk/java/src/main/java/dev/udb/client/UdbClient.java",
        (
            "public RecordSet select(SelectRequest request)",
            "public MutationResponse upsert(UpsertRequest request)",
            "public MutationResponse delete(DeleteRequest request)",
            "public UdbEntityHandle entity(",
            "public UdbEntityHandle table(",
            "GeneratedUdbClient.entities()",
            "DataBrokerGrpc.DataBrokerBlockingStub broker",
        ),
    ),
    SourceCheck(
        "java bound entity handle shapes DataBroker requests",
        "sdk/java/src/main/java/dev/udb/client/UdbEntityHandle.java",
        (
            "public final class UdbEntityHandle",
            "SelectRequest.newBuilder()",
            "UpsertRequest.newBuilder()",
            "DeleteRequest.newBuilder()",
            ".setContext(context(client.metadata()))",
            ".setMessageType(messageType)",
            ".setFilter(toStruct(where))",
            ".setRecordJson(ByteString.copyFromUtf8(toJson(record)))",
            ".setPayload(toStruct(record))",
            ".addAllConflictFields(key)",
            "Struct.newBuilder()",
            "selectJson(",
        ),
    ),
    SourceCheck(
        "java typed receipt/fence helper surface",
        "sdk/java/src/main/java/dev/udb/client/WriteReceipt.java",
        (
            "public record WriteReceipt(",
            "String sourceLsn",
            "long outboxSeq",
            "List<String> projectionTaskIds",
            "String manifestChecksum",
            "long writtenAtUnixMs",
            "public static WriteReceipt fromJson(String json)",
            "\"source_lsn\"",
            "\"projection_task_ids\"",
        ),
    ),
    SourceCheck(
        "java read fence maps source_lsn to min_outbox_lsn",
        "sdk/java/src/main/java/dev/udb/client/ReadFence.java",
        (
            "public record ReadFence(",
            "public static final long DEFAULT_MAX_WAIT_MS = 2500",
            "public static ReadFence fromReceipt(WriteReceipt receipt, long maxWaitMs)",
            "receipt.sourceLsn()",
            "receipt.projectionTaskIds()",
            "min_outbox_lsn",
            "max_wait_ms",
        ),
    ),
    SourceCheck(
        "java metadata emits one-shot read fence header",
        "sdk/java/src/main/java/dev/udb/client/UdbMetadata.java",
        (
            "String readFenceJson",
            "public UdbMetadata withReadFence(String readFenceJson)",
            "public UdbMetadata afterWrite(WriteReceipt receipt, long maxWaitMs)",
            "ReadFence.fromReceipt(receipt, maxWaitMs)",
        ),
    ),
    SourceCheck(
        "java client emits consistency headers",
        "sdk/java/src/main/java/dev/udb/client/UdbClient.java",
        (
            'Metadata.Key.of("x-udb-read-fence", Metadata.ASCII_STRING_MARSHALLER)',
            'Metadata.Key.of("x-udb-consistency", Metadata.ASCII_STRING_MARSHALLER)',
            "headers.put(READ_FENCE, meta.readFenceJson())",
            "headers.put(CONSISTENCY, meta.consistency())",
        ),
    ),
    SourceCheck(
        "java generated runtime exposes decoded error-detail helpers",
        "sdk/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java",
        (
            "Metadata.Key<byte[]> ERROR_DETAIL_KEY",
            "private final byte[] errorDetail;",
            "public byte[] errorDetail()",
            "private final transient ErrorDetail decodedErrorDetail;",
            "public ErrorDetail decodedErrorDetail()",
            "public boolean retryable()",
            "public long retryAfterMs()",
            "public ErrorKind kind()",
            "public java.util.List<java.util.Map<String, String>> fieldViolations()",
        ),
    ),
    SourceCheck(
        "csharp storage uploadFile helper",
        "sdk/csharp/Udb.Client/UdbStorageClient.cs",
        (
            "RegisterUploadAsync(",
            "FinalizeUploadAsync(",
            "GetDownloadUrlAsync(",
            "public async Task<StorageV1.FinalizeUploadResponse> UploadFileAsync(",
            "RegisterUpload -> HTTP PUT to the presigned URL -> FinalizeUpload",
            "registered.UploadUrl",
            "_putBytes(registered.UploadUrl, data, contentType, ct)",
            "FinalizeUploadAsync(finalize, headers, cancellationToken: ct)",
            "PutBytesAsync(",
        ),
    ),
    SourceCheck(
        "csharp project login-and-adopt workflow",
        "sdk/csharp/Udb.Client/UdbProject.cs",
        (
            "public UdbAuthClient Auth",
            "public UdbStorageClient Storage",
            "public void SetCredentials(",
            "public async Task<AuthnV1.AuthnResponse> LoginAndAdoptTenantAsync(",
            "var login = await Auth.LoginAsync(request, ct);",
            "var verified = await Auth.AuthenticateBearerAsync(token, ct);",
            "TenantId = string.IsNullOrWhiteSpace(principal.TenantId)",
            "Auth.UpdateMetadata(adopted);",
        ),
    ),
    SourceCheck(
        "csharp auth exposes Login wrapper and metadata rebind",
        "sdk/csharp/Udb.Client/UdbAuthClient.cs",
        (
            "public Task<AuthnV1.LoginResponse> LoginAsync(",
            "=> _authn.LoginAsync(request, Headers(), cancellationToken: ct).ResponseAsync",
            "internal void UpdateMetadata(UdbMetadata metadata)",
        ),
    ),
    SourceCheck(
        "csharp data client exposes bound entity/table helpers",
        "sdk/csharp/Udb.Client/UdbClient.cs",
        (
            "public Task<RecordSet> SelectAsync(",
            "public Task<MutationResponse> UpsertAsync(",
            "public Task<MutationResponse> DeleteAsync(",
            "public UdbEntityHandle Entity(",
            "public UdbEntityHandle Table(",
            "DataBroker.DataBrokerClient Broker",
        ),
    ),
    SourceCheck(
        "csharp bound entity handle shapes DataBroker requests",
        "sdk/csharp/Udb.Client/UdbEntityHandle.cs",
        (
            "public sealed class UdbEntityHandle",
            "new SelectRequest",
            "new UpsertRequest",
            "new DeleteRequest",
            "Context = Context(_metadata())",
            "MessageType = MessageType",
            "Filter = ToStruct(where)",
            "RecordJson = ByteString.CopyFromUtf8(RecordJson(record))",
            "Payload = ToStruct(record)",
            "request.ConflictFields.AddRange(Key)",
            "UdbIr.Entities",
            "DecodeRecords(",
        ),
    ),
    SourceCheck(
        "csharp typed receipt/fence helper surface",
        "sdk/csharp/Udb.Client/UdbClient.cs",
        (
            "public sealed record WriteReceipt(",
            '[property: JsonPropertyName("source_lsn")] string SourceLsn',
            '[property: JsonPropertyName("projection_task_ids")] string[]? ProjectionTaskIds',
            "public static WriteReceipt FromJson(string? json)",
            "public sealed record ReadFence(",
            "public const long DefaultMaxWaitMs = 2500",
            "public static ReadFence FromReceipt(WriteReceipt receipt, long maxWaitMs = DefaultMaxWaitMs)",
        ),
    ),
    SourceCheck(
        "csharp metadata emits one-shot read fence header",
        "sdk/csharp/Udb.Client/UdbClient.cs",
        (
            "string ReadFenceJson = \"\"",
            "public UdbMetadata WithReadFence(string readFenceJson)",
            "public UdbMetadata AfterWrite(WriteReceipt receipt, long maxWaitMs = ReadFence.DefaultMaxWaitMs)",
            'headers.Add("x-udb-read-fence", _metadata.ReadFenceJson)',
            'headers.Add("x-udb-consistency", _metadata.Consistency)',
        ),
    ),
    SourceCheck(
        "csharp generated runtime exposes decoded error-detail helpers",
        "sdk/csharp/Udb.Client/GeneratedClientRuntime.cs",
        (
            "public byte[]? ErrorDetail { get; }",
            "ValueBytes",
            "public global::Udb.Entity.V1.ErrorDetail? DecodedErrorDetail { get; }",
            "public bool Retryable => DecodedErrorDetail?.Retryable ?? false",
            "public long RetryAfterMs => DecodedErrorDetail?.RetryAfterMs ?? 0L",
            "public global::Udb.Entity.V1.ErrorKind Kind =>",
            "public System.Collections.Generic.IReadOnlyList<(string Field, string Description)> FieldViolations =>",
        ),
    ),
)


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    for check in CHECKS:
        path = root / check.path
        if not path.is_file():
            failures.append(f"{check.label}: missing file {check.path}")
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for token in check.required:
            if token not in text:
                failures.append(f"{check.label}: missing token {token!r} in {check.path}")
        for token in check.forbidden:
            if token in text:
                failures.append(f"{check.label}: forbidden now-fixed token {token!r} in {check.path}")
    return failures


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for check in CHECKS:
            path = root / check.path
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            path.write_text(existing + "\n".join(check.required) + "\n", encoding="utf-8")

        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        java_storage = root / "sdk/java/src/main/java/dev/udb/client/UdbStorageClient.java"
        java_storage.write_text(
            java_storage.read_text(encoding="utf-8").replace(
                "httpPut.put(registered.getUploadUrl(), data, opts.contentType())",
                "// missing PUT",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("java storage" in failure and "httpPut.put" in failure for failure in failures):
            raise AssertionError(f"expected Java uploadFile PUT drift failure, got {failures}")

        java_storage.write_text("\n".join(CHECKS[0].required) + "\n", encoding="utf-8")
        csharp_error = root / "sdk/csharp/Udb.Client/GeneratedClientRuntime.cs"
        csharp_error.write_text(
            csharp_error.read_text(encoding="utf-8").replace(
                "public bool Retryable", "public bool MissingRetryable"
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("csharp generated runtime" in failure and "public bool Retryable" in failure for failure in failures):
            raise AssertionError(f"expected C# decoded-error audit drift failure, got {failures}")

        csharp_generated_runtime = next(
            check for check in CHECKS if check.label == "csharp generated runtime exposes decoded error-detail helpers"
        )
        csharp_error.write_text("\n".join(csharp_generated_runtime.required) + "\n", encoding="utf-8")
        java_fence = root / "sdk/java/src/main/java/dev/udb/client/ReadFence.java"
        java_fence.write_text(java_fence.read_text(encoding="utf-8").replace("receipt.sourceLsn()", "receipt.manifestChecksum()"), encoding="utf-8")
        failures = check_source(root)
        if not any("java read fence" in failure and "receipt.sourceLsn()" in failure for failure in failures):
            raise AssertionError(f"expected Java read-fence mapping drift failure, got {failures}")

    print("Java/C# SDK audit selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo fixture assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()

    failures = check_source()
    if failures:
        for failure in failures:
            print(f"::error::{failure}")
        return 1
    print("Java/C# SDK audit guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
