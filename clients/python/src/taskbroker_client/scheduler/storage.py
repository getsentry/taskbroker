from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Protocol

from redis.client import StrictRedis
from rediscluster import RedisCluster

from taskbroker_client.metrics import MetricsBackend


class RunStorageProtocol(Protocol):
    """
    Storage interface for tracking the last run time of tasks.
    This is split out from `ScheduleRunner` to allow us to change storage
    in the future, or adapt taskworkers for other applications should we need to.
    """

    def set(self, key: str, next_runtime: datetime) -> bool:
        """
        Record a spawn time for a task.
        The next_runtime parameter indicates when the record should expire,
        and a task can be spawned again.

        Returns False when the key is set and a task should not be spawned.
        """
        ...

    def read(self, key: str) -> datetime | None:
        """
        Retrieve the last run time of a task
        Returns None if last run time has expired or is unknown.
        """
        ...

    def read_many(self, storage_keys: list[str]) -> Mapping[str, datetime | None]:
        """
        Retrieve last run times in bulk.

        Returns a mapping keyed by new storage_key.
        """
        ...

    def delete(self, key: str) -> None:
        """remove a task key - mostly for testing."""
        ...


class VolatileRunStorage(RunStorageProtocol):
    """
    An in-memory run storage implementation

    Provides no durability, and is not recommended for applications with timedelta
    schedules. Is a reasonable option for applications that use crontab schedules.
    """

    def __init__(self) -> None:
        self._data: dict[str, tuple[datetime, datetime]] = {}

    def set(self, key: str, next_runtime: datetime) -> bool:
        now = datetime.now(tz=UTC)

        if key not in self._data:
            self._data[key] = (now, next_runtime)
            return True
        existing_expires = self._data[key][1]
        if existing_expires <= now:
            self._data[key] = (now, next_runtime)
            return True
        return False

    def read(self, key: str) -> datetime | None:
        """
        Retrieve the last run time of a task
        Returns None if last run time has expired or is unknown.
        """
        value = self._data.get(key, None)
        if value is None:
            return None
        return value[0]

    def read_many(self, storage_keys: list[str]) -> Mapping[str, datetime | None]:
        """
        Retrieve last run times in bulk.

        Returns a mapping keyed by new storage_key.
        """
        results: dict[str, datetime | None] = {}
        for key in storage_keys:
            value = self._data.get(key, None)
            if value is None:
                results[key] = value
            else:
                results[key] = value[0]
        return results

    def delete(self, key: str) -> None:
        """remove a task key - mostly for testing."""
        self._data.pop(key, None)


class RunStorage(RunStorageProtocol):
    """
    Redis backed scheduler storage
    """

    def __init__(
        self, metrics: MetricsBackend, redis: RedisCluster[str] | StrictRedis[str]
    ) -> None:
        self._redis = redis
        self._metrics = metrics

    def _make_key(self, key: str) -> str:
        return f"tw:scheduler:{key}"

    def set(self, key: str, next_runtime: datetime) -> bool:
        """
        Record a spawn time for a task.
        The next_runtime parameter indicates when the record should expire,
        and a task can be spawned again.

        Returns False when the key is set and a task should not be spawned.
        """
        now = datetime.now(tz=UTC)
        # next_runtime & now could be the same second, and redis gets sad if ex=0
        duration = max(int((next_runtime - now).total_seconds()), 1)

        result = self._redis.set(self._make_key(key), now.isoformat(), ex=duration, nx=True)
        return bool(result)

    def read(self, key: str) -> datetime | None:
        """
        Retrieve the last run time of a task
        Returns None if last run time has expired or is unknown.
        """
        result = self._redis.get(self._make_key(key))
        if result:
            return datetime.fromisoformat(result)

        self._metrics.incr("taskworker.scheduler.run_storage.read.miss", tags={"taskname": key})
        return None

    def read_many(
        self,
        storage_keys: list[str],
    ) -> Mapping[str, datetime | None]:
        """
        Retrieve last run times in bulk.

        storage_keys are the new-format keys including the entry key prefix and
        schedule_id suffix (e.g. "my-entry:test:valid:300"). Falls back through
        two legacy formats to allow seamless deploys:

          new:    "{entry_key}:{fullname}:{schedule_id}"  (e.g. "my-entry:test:valid:300")
          compat: "{fullname}:{schedule_id}"              (e.g. "test:valid:300")
          legacy: "{fullname}"                            (e.g. "test:valid")

        Compat is derived by stripping the entry_key prefix (split on first colon).
        Legacy is derived from compat by stripping the schedule_id suffix (rsplit on last colon).

        Returns a mapping keyed by new storage_key.
        """
        compat_keys = [sk.split(":", 1)[1] for sk in storage_keys]
        legacy_keys = [ck.rsplit(":", 1)[0] for ck in compat_keys]

        new_values = self._redis.mget([self._make_key(sk) for sk in storage_keys])
        compat_values = self._redis.mget([self._make_key(ck) for ck in compat_keys])
        legacy_values = self._redis.mget([self._make_key(lk) for lk in legacy_keys])

        run_times: dict[str, datetime | None] = {}
        for storage_key, new_val, compat_val, legacy_val in zip(
            storage_keys, new_values, compat_values, legacy_values
        ):
            value = new_val
            if value is None:
                value = compat_val
            if value is None:
                value = legacy_val
            run_times[storage_key] = datetime.fromisoformat(value) if value else None
        return run_times

    def delete(self, key: str) -> None:
        """remove a task key - mostly for testing."""
        self._redis.delete(self._make_key(key))


RedisRunStorage = RunStorage
