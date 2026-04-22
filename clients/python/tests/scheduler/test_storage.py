from datetime import UTC, datetime

from taskbroker_client.scheduler.storage import VolatileRunStorage

from ..conftest import freeze_time


def test_volatile_run_storage_set_returns_true() -> None:
    storage = VolatileRunStorage()
    with freeze_time("2025-01-24 14:25:00 UTC"):
        result = storage.set("key", datetime(2025, 1, 24, 14, 30, 0, tzinfo=UTC))
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
