using Google.Protobuf;
using Grpc.Core;
using Udb.Client.Generated;
using Xunit;
using EntityV1 = Udb.Entity.V1;

namespace Udb.Client.Tests;

public sealed class UdbRpcExceptionTests
{
    [Fact]
    public void Decodes_ErrorDetail_Trailer_FieldViolations_And_TypedAccessors()
    {
        var detail = new EntityV1.ErrorDetail
        {
            Retryable = false,
            RetryAfterMs = 0,
            Kind = EntityV1.ErrorKind.Validation,
        };
        detail.FieldViolations.Add(new EntityV1.ErrorFieldViolation
        {
            Field = "email",
            Description = "must be a valid email",
        });
        var trailers = new Metadata
        {
            { "udb-error-detail-bin", detail.ToByteArray() },
        };
        var rpc = new RpcException(
            new Status(StatusCode.InvalidArgument, "validation failed"),
            trailers);

        var ex = new UdbRpcException("/svc/DoThing", rpc);

        Assert.Equal(StatusCode.InvalidArgument, ex.Code);
        Assert.NotNull(ex.ErrorDetail);
        Assert.NotNull(ex.DecodedErrorDetail);
        Assert.False(ex.Retryable);
        Assert.Equal(0, ex.RetryAfterMs);
        Assert.Equal(EntityV1.ErrorKind.Validation, ex.Kind);
        var violation = Assert.Single(ex.FieldViolations);
        Assert.Equal("email", violation.Field);
        Assert.Equal("must be a valid email", violation.Description);
    }

    [Fact]
    public void Decodes_Quota_ErrorDetail_Retry_Backoff()
    {
        var detail = new EntityV1.ErrorDetail
        {
            Backend = "admission",
            Operation = "tenant budget",
            Retryable = true,
            RetryAfterMs = 250,
            Kind = EntityV1.ErrorKind.Quota,
        };
        var trailers = new Metadata
        {
            { "udb-error-detail-bin", detail.ToByteArray() },
        };
        var rpc = new RpcException(
            new Status(StatusCode.ResourceExhausted, "quota"),
            trailers);

        var ex = new UdbRpcException("/svc/DoThing", rpc);

        Assert.True(ex.Retryable);
        Assert.Equal(250, ex.RetryAfterMs);
        Assert.Equal(EntityV1.ErrorKind.Quota, ex.Kind);
        Assert.Empty(ex.FieldViolations);
    }

    [Fact]
    public void Synthesizes_Transport_ErrorDetail_When_Trailer_Is_Absent()
    {
        var rpc = new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline"));

        var ex = new UdbRpcException("/svc/DoThing", rpc);

        Assert.Null(ex.ErrorDetail);
        Assert.NotNull(ex.DecodedErrorDetail);
        Assert.True(ex.Retryable);
        Assert.Equal("transport", ex.DecodedErrorDetail!.Backend);
        Assert.Equal("deadline_exceeded", ex.DecodedErrorDetail!.Operation);
        Assert.Equal(0, ex.RetryAfterMs);
        Assert.Equal(EntityV1.ErrorKind.Retryable, ex.Kind);
        Assert.Empty(ex.FieldViolations);
    }

    [Fact]
    public void Synthesizes_Cancelled_Transport_ErrorDetail_As_Not_Retryable()
    {
        var rpc = new RpcException(new Status(StatusCode.Cancelled, "cancelled"));

        var ex = new UdbRpcException("/svc/DoThing", rpc);

        Assert.Null(ex.ErrorDetail);
        Assert.NotNull(ex.DecodedErrorDetail);
        Assert.False(ex.Retryable);
        Assert.Equal("transport", ex.DecodedErrorDetail!.Backend);
        Assert.Equal("cancelled", ex.DecodedErrorDetail!.Operation);
        Assert.Equal(0, ex.RetryAfterMs);
        Assert.Equal(EntityV1.ErrorKind.Retryable, ex.Kind);
        Assert.Empty(ex.FieldViolations);
    }
}
