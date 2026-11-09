using System.Reflection;
using Google.Protobuf;
using Grpc.Core;
using Udb.Client.Generated;
using Xunit;
using EntityV1 = Udb.Entity.V1;

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

    [Fact]
    public async Task ServerRetryAfterIsHonouredBeforeTheNextAttempt()
    {
        // Repeated: one lucky jitter draw must not be what makes this pass.
        for (var run = 0; run < 3; run++)
        {
            var probe = new ScriptedUnaryProbe(new UdbCallOptions(), Throttled(1200), null);

            var response = await probe.CallAsync(deadline: null);

            Assert.Equal("ok", response.Backend);
            Assert.Equal(2, probe.Calls);
            var wait = Assert.Single(probe.Waits);
            Assert.True(
                wait >= TimeSpan.FromMilliseconds(1200),
                $"run {run}: waited {wait.TotalMilliseconds}ms, broker asked for 1200ms");
        }
    }

    [Fact]
    public async Task ServerRetryAfterPastTheDeadlineThrowsWithoutSleeping()
    {
        var probe = new ScriptedUnaryProbe(new UdbCallOptions(), Throttled(1200), null);

        var ex = await Assert.ThrowsAsync<UdbRpcException>(
            () => probe.CallAsync(deadline: TimeSpan.FromMilliseconds(500)));

        Assert.Equal(StatusCode.ResourceExhausted, ex.Code);
        Assert.Equal(1200, ex.RetryAfterMs);
        Assert.Equal(1, probe.Calls);
        Assert.Empty(probe.Waits);
    }

    [Theory]
    [InlineData(1500, 5000)]
    [InlineData(-1, 60000)] // -1: the default MaxRetryAfter (10 s)
    public async Task ServerRetryAfterAboveTheCapThrowsWithoutSleeping(int maxRetryAfterMs, long retryAfterMs)
    {
        var options = maxRetryAfterMs < 0
            ? new UdbCallOptions { InitialBackoff = TimeSpan.FromMilliseconds(1) }
            : new UdbCallOptions
            {
                InitialBackoff = TimeSpan.FromMilliseconds(1),
                MaxRetryAfter = TimeSpan.FromMilliseconds(maxRetryAfterMs),
            };
        var probe = new ScriptedUnaryProbe(options, Throttled(retryAfterMs), null);

        var ex = await Assert.ThrowsAsync<UdbRpcException>(() => probe.CallAsync(deadline: null));

        Assert.Equal(StatusCode.ResourceExhausted, ex.Code);
        Assert.Equal(retryAfterMs, ex.RetryAfterMs);
        Assert.Equal(1, probe.Calls);
        Assert.Empty(probe.Waits);
    }

    [Fact]
    public void MaxRetryAfterDefaultsToTenSeconds()
    {
        Assert.Equal(TimeSpan.FromSeconds(10), new UdbCallOptions().MaxRetryAfter);
    }

    private static RpcException Throttled(long retryAfterMs)
    {
        var detail = new EntityV1.ErrorDetail
        {
            Backend = "admission",
            Operation = "rate limit",
            Retryable = true,
            RetryAfterMs = retryAfterMs,
            Kind = EntityV1.ErrorKind.Quota,
        };
        var trailers = new Metadata
        {
            { "udb-error-detail-bin", detail.ToByteArray() },
        };
        return new RpcException(new Status(StatusCode.ResourceExhausted, "slow down"), trailers);
    }

    /// <summary>
    /// Drives the real unary retry loop against a scripted sequence of outcomes
    /// (a null entry answers OK) and records every requested wait instead of sleeping.
    /// </summary>
    private sealed class ScriptedUnaryProbe : GeneratedServiceBase
    {
        private readonly Queue<RpcException?> _script;

        public ScriptedUnaryProbe(UdbCallOptions options, params RpcException?[] script)
            : base(() => new Metadata(), options)
        {
            _script = new Queue<RpcException?>(script);
            RetryDelay = (wait, _) =>
            {
                Waits.Add(wait);
                return Task.CompletedTask;
            };
        }

        public List<TimeSpan> Waits { get; } = new();

        public int Calls { get; private set; }

        public async Task<EntityV1.ErrorDetail> CallAsync(TimeSpan? deadline)
        {
            dynamic result = await InvokeUnaryAsync(
                "/udb.test.v1.Probe/Read",
                _ => Next(),
                deadline,
                CancellationToken.None,
                readOnly: true,
                replaySafe: false,
                request: null);
            return (EntityV1.ErrorDetail)result;
        }

        private object Next()
        {
            Calls++;
            var failure = _script.Dequeue();
            var response = failure is null
                ? Task.FromResult(new EntityV1.ErrorDetail { Backend = "ok" })
                : Task.FromException<EntityV1.ErrorDetail>(failure);
            return new AsyncUnaryCall<EntityV1.ErrorDetail>(
                response,
                Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess,
                () => new Metadata(),
                () => { });
        }
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
