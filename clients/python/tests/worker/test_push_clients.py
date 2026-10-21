import pytest

from taskbroker_client.metrics import NoOpMetricsBackend
from taskbroker_client.worker.push_clients import PushTaskbrokerClient, service_authority


@pytest.mark.parametrize(
    "service,expected",
    [
        ("task-ingest-push-broker-grpc:50051", "task-ingest-push-broker-grpc"),
        ("task-ingest-push-broker-grpc:80", "task-ingest-push-broker-grpc"),
        ("localhost:50051", "localhost"),
        ("[::1]:50051", "[::1]"),
        ("task-ingest-push-broker-grpc", "task-ingest-push-broker-grpc"),
    ],
)
def test_service_authority_strips_the_port(service: str, expected: str) -> None:
    assert service_authority(service) == expected


def test_client_sets_default_authority() -> None:
    client = PushTaskbrokerClient(
        service="task-ingest-push-broker-grpc:50051",
        application="tests",
        metrics=NoOpMetricsBackend(),
    )
    assert (
        "grpc.default_authority",
        "task-ingest-push-broker-grpc",
    ) in client._grpc_options
