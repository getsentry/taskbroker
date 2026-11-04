from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest import mock

import pytest
from redis.cluster import PRIMARY, ClusterNode, NodesManager, RedisCluster
from redis.crc import REDIS_CLUSTER_HASH_SLOTS
from redis.exceptions import RedisClusterException

from taskbroker_client.metrics import NoOpMetricsBackend
from taskbroker_client.scheduler.storage import RunStorage, VolatileRunStorage

from ..conftest import freeze_time


def test_volatile_run_storage_set() -> None:
    storage = VolatileRunStorage()
    with freeze_time("2025-01-24 14:25:00 UTC"):
        result = storage.set("key", datetime(2025, 1, 24, 14, 30, 0, tzinfo=UTC))
        assert result is True
    with freeze_time("2025-01-24 14:26:00 UTC"):
        result = storage.set("key", datetime(2025, 1, 24, 14, 30, 0, tzinfo=UTC))
        assert result is False
    with freeze_time("2025-01-24 14:30:01 UTC"):
        result = storage.set("key", datetime(2025, 1, 24, 14, 35, 0, tzinfo=UTC))
        assert result is True


def test_volatile_run_storage_set_stores_now_not_next_runtime() -> None:
    storage = VolatileRunStorage()
    now = datetime(2025, 1, 24, 14, 25, 0, tzinfo=UTC)
    next_runtime = datetime(2025, 1, 24, 14, 30, 0, tzinfo=UTC)
    with freeze_time(now):
        storage.set("key", next_runtime)
    result = storage.read("key")
    assert result == now


def test_volatile_run_storage_read_after_set() -> None:
    storage = VolatileRunStorage()
    now = datetime(2025, 1, 24, 14, 25, 0, tzinfo=UTC)
    with freeze_time(now):
        storage.set("key", datetime(2025, 1, 24, 14, 30, 0, tzinfo=UTC))
    assert storage.read("key") == now
    assert storage.read("not-defined") is None


def test_volatile_run_storage_read_many_mixed() -> None:
    storage = VolatileRunStorage()
    t1 = datetime(2025, 1, 24, 14, 25, 0, tzinfo=UTC)
    with freeze_time(t1):
        storage.set("present-1", datetime(2025, 1, 24, 14, 30, 0, tzinfo=UTC))
        storage.set("present-2", datetime(2025, 1, 24, 14, 31, 0, tzinfo=UTC))

    result = storage.read_many(["present-1", "missing", "present-2"])
    assert result == {"present-1": t1, "missing": None, "present-2": t1}

    result = storage.read_many(["a", "b", "c"])
    assert result == {"a": None, "b": None, "c": None}


def test_volatile_run_storage_delete() -> None:
    storage = VolatileRunStorage()
    with freeze_time("2025-01-24 14:25:00 UTC"):
        storage.set("key", datetime(2025, 1, 24, 14, 30, 0, tzinfo=UTC))
    assert storage.read("key") is not None

    storage.delete("key")
    storage.delete("missing")
    assert storage.read("key") is None


class SeerRedisCluster(RedisCluster):
    """Workaround used by Seer: ``mget`` keys across hash slots, like redis-py-cluster did."""

    def mget(self, keys: Any, *args: Any) -> Any:
        return self.mget_nonatomic(keys, *args)


def _single_server_slots(self: NodesManager) -> None:
    # CI's redis isn't a cluster, so route every slot to it. The slot check that
    # fails is client-side, so this reproduces what a real cluster does.
    node = ClusterNode("localhost", 6379, server_type=PRIMARY)
    node.redis_connection = self.create_redis_node("localhost", 6379, **self.connection_kwargs)
    self.nodes_cache = {node.name: node}
    self.slots_cache = {slot: [node] for slot in range(REDIS_CLUSTER_HASH_SLOTS)}
    self.default_node = node  # type: ignore[assignment]


@pytest.fixture
def cluster_storage(request: pytest.FixtureRequest) -> Iterator[RunStorage]:
    with mock.patch.object(NodesManager, "initialize", _single_server_slots):
        client = request.param(host="localhost", port=6379, decode_responses=True)
    storage = RunStorage(metrics=NoOpMetricsBackend(), redis=client)
    keys = ["first:test:valid:300", "second:test:valid:300"]
    yield storage
    for key in keys:
        storage.delete(key)


@pytest.mark.parametrize("cluster_storage", [RedisCluster], indirect=True)
def test_run_storage_read_many_fails_on_redis_cluster(cluster_storage: RunStorage) -> None:
    cluster_storage.set("first:test:valid:300", datetime(2099, 1, 1, tzinfo=UTC))

    with pytest.raises(RedisClusterException, match="all keys must map to the same key slot"):
        cluster_storage.read_many(["first:test:valid:300", "second:test:valid:300"])


@pytest.mark.parametrize("cluster_storage", [SeerRedisCluster], indirect=True)
def test_run_storage_read_many_with_cross_slot_mget(cluster_storage: RunStorage) -> None:
    cluster_storage.set("first:test:valid:300", datetime(2099, 1, 1, tzinfo=UTC))
    first_run = cluster_storage.read("first:test:valid:300")

    result = cluster_storage.read_many(["first:test:valid:300", "second:test:valid:300"])

    assert first_run is not None
    assert result == {"first:test:valid:300": first_run, "second:test:valid:300": None}
