import grpc

from udb_client.generated_client import RetryPolicy


def test_retry_policy_does_not_retry_mutations() -> None:
    policy = RetryPolicy(max_attempts=4)

    for code in (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ):
        assert not policy.should_retry(code, 1, read_only=False)


def test_retry_policy_keeps_read_only_transient_retries() -> None:
    policy = RetryPolicy(max_attempts=4)

    for code in (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ):
        assert policy.should_retry(code, 1, read_only=True)
    assert not policy.should_retry(grpc.StatusCode.INVALID_ARGUMENT, 1, read_only=True)
