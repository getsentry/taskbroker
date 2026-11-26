from collections.abc import Iterator
from datetime import UTC, datetime
from unittest import mock

import pytest

# redis-py only ships RedisCluster from 4.1; the locked dev version may be older.
pytest.importorskip("redis.cluster")

from redis.cluster import PRIMARY, ClusterNode, NodesManager, RedisCluster  # noqa: E402
from redis.crc import REDIS_CLUSTER_HASH_SLOTS  # noqa: E402

from taskbroker_client.metrics import NoOpMetricsBackend  # noqa: E402
from taskbroker_client.scheduler.storage import RunStorage  # noqa: E402


def _single_server_slots(self: NodesManager) -> None:
    # CI's redis isn't a cluster, so route every slot to it. The slot check that
    # fails is client-side, so this reproduces what a real cluster does.
    node = ClusterNode("localhost", 6379, server_type=PRIMARY)
    node.redis_connection = self.create_redis_node("localhost", 6379, **self.connection_kwargs)
    self.nodes_cache = {node.name: node}
    self.slots_cache = {slot: [node] for slot in range(REDIS_CLUSTER_HASH_SLOTS)}
    self.default_node = node


@pytest.fixture
def cluster_storage() -> Iterator[RunStorage]:
    with mock.patch.object(NodesManager, "initialize", _single_server_slots):
        client = RedisCluster(host="localhost", port=6379, decode_responses=True)
    client.flushdb()
    yield RunStorage(metrics=NoOpMetricsBackend(), redis=client)


def test_run_storage_read_many_on_redis_cluster(cluster_storage: RunStorage) -> None:
    cluster_storage.set("first:test:valid:300", datetime(2099, 1, 1, tzinfo=UTC))
    first_run = cluster_storage.read("first:test:valid:300")

    result = cluster_storage.read_many(["first:test:valid:300", "second:test:valid:300"])

    assert first_run is not None
    assert result == {"first:test:valid:300": first_run, "second:test:valid:300": None}
