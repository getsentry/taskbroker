from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from redis import StrictRedis

from taskbroker_client.app import TaskbrokerApp
from taskbroker_client.metrics import NoOpMetricsBackend
from taskbroker_client.scheduler.config import crontab
from taskbroker_client.scheduler.runner import RunStorage, ScheduleRunner

from ..conftest import freeze_time, producer_factory


@pytest.fixture
def task_app() -> TaskbrokerApp:
    app = TaskbrokerApp(name="acme", producer_factory=producer_factory)
    namespace = app.taskregistry.create_namespace("test")

    @namespace.register(name="valid")
    def test_func() -> None:
        pass

    @namespace.register(name="second")
    def second_func() -> None:
        pass

    return app


@pytest.fixture
def run_storage() -> RunStorage:
    # TODO use env vars for redis port.
    redis = StrictRedis(host="localhost", port=6379, decode_responses=True)
    redis.flushdb()
    return RunStorage(metrics=NoOpMetricsBackend(), redis=redis)


def test_runstorage_zero_duration(run_storage: RunStorage) -> None:
    with freeze_time("2025-07-19 14:25:00"):
        now = datetime.now(tz=UTC)
        result = run_storage.set("test:do_stuff", now)
        assert result is True


def test_runstorage_double_set(run_storage: RunStorage) -> None:
    with freeze_time("2025-07-19 14:25:00"):
        now = datetime.now(tz=UTC)
        first = run_storage.set("test:do_stuff", now)
        second = run_storage.set("test:do_stuff", now)

        assert first is True, "initial set should return true"
        assert second is False, "writing a key that exists should fail"


def test_schedulerunner_add_invalid(task_app: TaskbrokerApp) -> None:
    run_storage = Mock(spec=RunStorage)
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)

    with pytest.raises(ValueError) as err:
        schedule_set.add(
            "invalid",
            {
                "task": "invalid",
                "schedule": timedelta(minutes=5),
            },
        )
    assert "Invalid task name" in str(err)

    with pytest.raises(KeyError) as key_err:
        schedule_set.add(
            "invalid",
            {
                "task": "test:invalid",
                "schedule": timedelta(minutes=5),
            },
        )
    assert "No task registered" in str(key_err)

    with pytest.raises(ValueError) as err:
        schedule_set.add(
            "valid",
            {
                "task": "test:valid",
                "schedule": timedelta(microseconds=99),
            },
        )
    assert "microseconds" in str(err)


def test_schedulerunner_tick_no_tasks(task_app: TaskbrokerApp, run_storage: RunStorage) -> None:
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)

    with freeze_time("2025-01-24 14:25:00 UTC"):
        sleep_time = schedule_set.tick()
        assert sleep_time == 60


def test_schedulerunner_tick_one_task_time_remaining(
    task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)

    schedule_set.add(
        "valid",
        {
            "task": "test:valid",
            "schedule": timedelta(minutes=5),
        },
    )
    # Last run was two minutes ago.
    with freeze_time("2025-01-24 14:23:00 UTC"):
        run_storage.set("test:valid:300", datetime(2025, 1, 24, 14, 28, 0, tzinfo=UTC))

    namespace = task_app.taskregistry.get("test")
    with freeze_time("2025-01-24 14:25:00 UTC"), patch.object(namespace, "send_task") as mock_send:
        sleep_time = schedule_set.tick()
        assert sleep_time == 180
        assert mock_send.call_count == 0

    last_run = run_storage.read("test:valid:300")
    assert last_run == datetime(2025, 1, 24, 14, 23, 0, tzinfo=UTC)


def test_schedulerunner_tick_one_task_spawned(
    task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    run_storage = Mock(spec=RunStorage)
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)
    schedule_set.add(
        "valid",
        {
            "task": "test:valid",
            "schedule": timedelta(minutes=5),
        },
    )

    # Last run was 5 minutes from the freeze_time below
    run_storage.read_many.return_value = {
        "test:valid:300": datetime(2025, 1, 24, 14, 19, 55, tzinfo=UTC),
    }
    run_storage.set.return_value = True

    namespace = task_app.taskregistry.get("test")
    with freeze_time("2025-01-24 14:25:00 UTC"), patch.object(namespace, "send_task") as mock_send:
        sleep_time = schedule_set.tick()
        assert sleep_time == 300
        assert mock_send.call_count == 1

        # scheduled tasks should not continue the scheduler trace
        send_args = mock_send.call_args
        assert send_args.args[0].headers["sentry-propagate-traces"] == "False"
        assert "sentry-trace" not in send_args.args[0].headers

    assert run_storage.set.call_count == 1
    # set() is called with the correct next_run time
    run_storage.set.assert_called_with(
        "test:valid:300", datetime(2025, 1, 24, 14, 30, 0, tzinfo=UTC)
    )


@patch("taskbroker_client.scheduler.runner.capture_checkin")
def test_schedulerunner_tick_create_checkin(
    mock_capture_checkin: Mock, task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    run_storage = Mock(spec=RunStorage)
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)
    schedule_set.add(
        "important-task",
        {
            "task": "test:valid",
            "schedule": timedelta(minutes=5),
        },
    )

    # Last run was 5 minutes from the freeze_time below
    run_storage.read_many.return_value = {
        "test:valid:300": datetime(2025, 1, 24, 14, 19, 55, tzinfo=UTC),
    }
    run_storage.set.return_value = True
    mock_capture_checkin.return_value = "checkin-id"

    namespace = task_app.taskregistry.get("test")
    with (
        freeze_time("2025-01-24 14:25:00 UTC"),
        patch.object(namespace, "send_task") as mock_send,
    ):
        sleep_time = schedule_set.tick()
        assert sleep_time == 300

        assert mock_send.call_count == 1

        # assert that the activation had the correct headers
        send_args = mock_send.call_args
        assert "sentry-monitor-check-in-id" in send_args.args[0].headers
        assert send_args.args[0].headers["sentry-monitor-slug"] == "important-task"
        assert send_args.args[0].headers["sentry-propagate-traces"] == "False"
        assert "sentry-trace" not in send_args.args[0].headers

        # Ensure a checkin was created
        assert mock_capture_checkin.call_count == 1
        mock_capture_checkin.assert_called_with(
            monitor_slug="important-task",
            monitor_config={
                "schedule": {
                    "type": "interval",
                    "unit": "minute",
                    "value": 5,
                },
                "timezone": "UTC",
            },
            status="in_progress",
        )


def test_schedulerunner_tick_key_exists_no_spawn(
    task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)
    schedule_set.add(
        "valid",
        {
            "task": "test:valid",
            "schedule": timedelta(minutes=5),
        },
    )

    namespace = task_app.taskregistry.get("test")
    with patch.object(namespace, "send_task") as mock_send, freeze_time("2025-01-24 14:25:00 UTC"):
        # Run tick() to initialize state in the scheduler. This will write a key to run_storage.
        sleep_time = schedule_set.tick()
        assert sleep_time == 300
        assert mock_send.call_count == 1

    with freeze_time("2025-01-24 14:30:00 UTC"):
        # Set a key into run_storage to simulate another scheduler running
        run_storage.delete("test:valid:300")
        assert run_storage.set("test:valid:300", datetime.now(tz=UTC) + timedelta(minutes=2))

    # Our scheduler would wakeup and tick again.
    # The key exists in run_storage so we should not spawn a task.
    # last_run time should synchronize with run_storage state, and count down from 14:30
    with freeze_time("2025-01-24 14:30:02 UTC"):
        sleep_time = schedule_set.tick()
        assert sleep_time == 298
        assert mock_send.call_count == 1


def test_schedulerunner_tick_one_task_multiple_ticks(
    task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)
    schedule_set.add(
        "valid",
        {
            "task": "test:valid",
            "schedule": timedelta(minutes=5),
        },
    )

    with freeze_time("2025-01-24 14:25:00 UTC"):
        sleep_time = schedule_set.tick()
        assert sleep_time == 300

    with freeze_time("2025-01-24 14:26:00 UTC"):
        sleep_time = schedule_set.tick()
        assert sleep_time == 240

    with freeze_time("2025-01-24 14:28:00 UTC"):
        sleep_time = schedule_set.tick()
        assert sleep_time == 120


def test_schedulerunner_tick_one_task_multiple_ticks_crontab(
    task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)
    schedule_set.add(
        "valid",
        {
            "task": "test:valid",
            "schedule": crontab(minute="*/2"),
        },
    )

    namespace = task_app.taskregistry.get("test")
    with patch.object(namespace, "send_task") as mock_send:
        with freeze_time("2025-01-24 14:24:00 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 120
        assert mock_send.call_count == 1

        with freeze_time("2025-01-24 14:25:00 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 60

        # Remove key to simulate expiration
        run_storage.delete("test:valid:*/2_*_*_*_*")
        with freeze_time("2025-01-24 14:26:00 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 120
        assert mock_send.call_count == 2


def test_schedulerunner_tick_multiple_tasks(
    task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)
    schedule_set.add(
        "valid",
        {
            "task": "test:valid",
            "schedule": timedelta(minutes=5),
        },
    )
    schedule_set.add(
        "second",
        {
            "task": "test:second",
            "schedule": timedelta(minutes=2),
        },
    )

    namespace = task_app.taskregistry.get("test")
    with patch.object(namespace, "send_task") as mock_send:
        with freeze_time("2025-01-24 14:25:00 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 120

        assert mock_send.call_count == 2

        with freeze_time("2025-01-24 14:26:00 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 60

        assert mock_send.call_count == 2

        # Remove the redis key, as the ttl in redis doesn't respect freeze_time()
        run_storage.delete("test:second:120")
        with freeze_time("2025-01-24 14:27:01 UTC"):
            sleep_time = schedule_set.tick()
            # two minutes left on the 5 min task
            assert sleep_time == 120

        assert mock_send.call_count == 3


def test_schedulerunner_tick_fast_and_slow(
    task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)
    schedule_set.add(
        "valid",
        {
            "task": "test:valid",
            "schedule": timedelta(seconds=30),
        },
    )
    schedule_set.add(
        "second",
        {
            "task": "test:second",
            "schedule": crontab(minute="*/2"),
        },
    )

    namespace = task_app.taskregistry.get("test")
    with patch.object(namespace, "send_task") as mock_send:
        with freeze_time("2025-01-24 14:25:00 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 30

        called = extract_sent_tasks(mock_send)
        assert called == ["valid"]

        run_storage.delete("test:valid:30")
        with freeze_time("2025-01-24 14:25:30 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 30

        called = extract_sent_tasks(mock_send)
        assert called == ["valid", "valid"]

        run_storage.delete("test:valid:30")
        with freeze_time("2025-01-24 14:26:00 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 30

        called = extract_sent_tasks(mock_send)
        assert called == ["valid", "valid", "second", "valid"]

        run_storage.delete("test:valid:30")
        with freeze_time("2025-01-24 14:26:30 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 30

        called = extract_sent_tasks(mock_send)
        assert called == ["valid", "valid", "second", "valid", "valid"]

        run_storage.delete("test:valid:30")
        with freeze_time("2025-01-24 14:27:00 UTC"):
            sleep_time = schedule_set.tick()
            assert sleep_time == 30

        assert run_storage.read("test:valid:30")
        called = extract_sent_tasks(mock_send)
        assert called == [
            "valid",
            "valid",
            "second",
            "valid",
            "valid",
            "valid",
        ]


def extract_sent_tasks(mock: Mock) -> list[str]:
    return [call[0][0].taskname for call in mock.call_args_list]


def test_scheduleentry_storage_key(task_app: TaskbrokerApp) -> None:
    run_storage = Mock(spec=RunStorage)
    runner = ScheduleRunner(app=task_app, run_storage=run_storage)
    runner.add("valid", {"task": "test:valid", "schedule": timedelta(minutes=5)})
    entry = runner._entries[0]
    assert entry.storage_key == "test:valid:300"
    assert entry.fullname == "test:valid"

    runner2 = ScheduleRunner(app=task_app, run_storage=run_storage)
    runner2.add("valid", {"task": "test:valid", "schedule": crontab(minute="*/2")})
    entry2 = runner2._entries[0]
    assert entry2.storage_key == "test:valid:*/2_*_*_*_*"


def test_schedulerunner_schedule_change_spawns_immediately(
    task_app: TaskbrokerApp, run_storage: RunStorage
) -> None:
    """
    When a schedule is changed, the old key (with a different schedule_id suffix)
    should not block spawning. The task should run immediately on the new schedule.
    """
    # Simulate the old scheduler having stored state with a 3-hour schedule
    old_storage_key = "test:valid:10800"  # 3 hours = 10800 seconds
    with freeze_time("2025-01-24 12:00:00 UTC"):
        # Old key with a 3h TTL would normally block for up to 3 more hours
        run_storage.set(old_storage_key, datetime(2025, 1, 24, 15, 0, 0, tzinfo=UTC))

    # New scheduler with a changed schedule (10 minutes = 600s)
    schedule_set = ScheduleRunner(app=task_app, run_storage=run_storage)
    schedule_set.add("valid", {"task": "test:valid", "schedule": timedelta(minutes=10)})

    namespace = task_app.taskregistry.get("test")
    with freeze_time("2025-01-24 14:25:00 UTC"), patch.object(namespace, "send_task") as mock_send:
        sleep_time = schedule_set.tick()
        # Task should spawn immediately — no last_run found under the new key
        assert mock_send.call_count == 1
        assert sleep_time == 600  # 10 minutes


def test_runstorage_read_many_backwards_compat(run_storage: RunStorage) -> None:
    """
    read_many() should fall back to the legacy key (old format without schedule_id suffix)
    when the new-format key has no data. When the new key exists it should take precedence.
    """
    with freeze_time("2025-01-24 14:25:00 UTC"):
        now = datetime.now(tz=UTC)
        # Write state under the old legacy key format (no schedule_id suffix)
        run_storage._redis.set(
            run_storage._make_key("test:valid"),
            now.isoformat(),
            ex=300,
        )

    # New-format key doesn't exist yet — should fall back to legacy value
    result = run_storage.read_many(["test:valid:300"])
    assert result["test:valid:300"] == now

    # Once a new-format key is written, it wins over the legacy key
    with freeze_time("2025-01-24 14:26:00 UTC"):
        new_time = datetime.now(tz=UTC)
        run_storage._redis.set(
            run_storage._make_key("test:valid:300"),
            new_time.isoformat(),
            ex=300,
        )
    result2 = run_storage.read_many(["test:valid:300"])
    assert result2["test:valid:300"] == new_time
