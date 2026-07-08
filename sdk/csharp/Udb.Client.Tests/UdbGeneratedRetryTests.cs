using System.Reflection;
using Grpc.Core;
using Udb.Client.Generated;
using Xunit;

namespace Udb.Client.Tests;

public sealed class UdbGeneratedRetryTests
{
    private readonly RetryProbe _probe = new();

    [Fact]
    public void ReplaySafeMutationWithKeyRetries()
    {
        Assert.True(_probe.IsRetryable(StatusCode.Unavailable, readOnly: false, replaySafe: true, hasIdempotencyKey: true));
        Assert.True(_probe.IsRetryable(StatusCode.ResourceExhausted, readOnly: false, replaySafe: true, hasIdempotencyKey: true));
    }

    [Fact]
    public void ReplaySafeMutationWithoutKeyDoesNotRetry()
    {
        Assert.False(_probe.IsRetryable(StatusCode.Unavailable, readOnly: false, replaySafe: true, hasIdempotencyKey: false));
    }

    [Fact]
    public void NonReplaySafeMutationWithKeyDoesNotRetry()
    {
        Assert.False(_probe.IsRetryable(StatusCode.Unavailable, readOnly: false, replaySafe: false, hasIdempotencyKey: true));
    }

    [Fact]
    public void DeadlineExceededMutationDoesNotRetry()
    {
        Assert.False(_probe.IsRetryable(StatusCode.DeadlineExceeded, readOnly: false, replaySafe: true, hasIdempotencyKey: true));
    }

    [Fact]
    public void ReadOnlyRetryBehaviorIsUnchanged()
    {
        Assert.True(_probe.IsRetryable(StatusCode.DeadlineExceeded, readOnly: true, replaySafe: false, hasIdempotencyKey: false));
        Assert.True(_probe.IsRetryable(StatusCode.Unavailable, readOnly: true, replaySafe: false, hasIdempotencyKey: false));
    }

    [Fact]
    public void DetectsNonEmptyIdempotencyKeyOnly()
    {
        Assert.True(RetryProbe.HasIdempotencyKey(new { IdempotencyKey = "idem-1" }));
        Assert.False(RetryProbe.HasIdempotencyKey(new { IdempotencyKey = "   " }));
        Assert.False(RetryProbe.HasIdempotencyKey(new { Other = "idem-1" }));
        Assert.False(RetryProbe.HasIdempotencyKey(null));
    }

    [Fact]
    public void BlankIdempotencyKeyDoesNotSatisfyMutationRetry()
    {
        var hasKey = RetryProbe.HasIdempotencyKey(new { IdempotencyKey = "   " });

        Assert.False(hasKey);
        Assert.False(_probe.IsRetryable(StatusCode.Unavailable, readOnly: false, replaySafe: true, hasIdempotencyKey: hasKey));
    }

    private sealed class RetryProbe : GeneratedServiceBase
    {
        private static readonly MethodInfo RetryableMethod =
            typeof(GeneratedServiceBase).GetMethod(
                "IsRetryable",
                BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(GeneratedServiceBase), "IsRetryable");

        private static readonly MethodInfo HasKeyMethod =
            typeof(GeneratedServiceBase).GetMethod(
                "HasIdempotencyKey",
                BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(GeneratedServiceBase), "HasIdempotencyKey");

        public RetryProbe()
            : base(() => new Metadata(), new UdbCallOptions())
        {
        }

        public bool IsRetryable(StatusCode code, bool readOnly, bool replaySafe, bool hasIdempotencyKey)
        {
            return (bool)RetryableMethod.Invoke(
                this,
                new object[] { code, readOnly, replaySafe, hasIdempotencyKey })!;
        }

        public static bool HasIdempotencyKey(object? request)
        {
            return (bool)HasKeyMethod.Invoke(null, new[] { request })!;
        }
    }
}
