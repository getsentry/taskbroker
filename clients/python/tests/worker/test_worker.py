import contextlib
import os
import queue
import signal
import threading
import time
from collections.abc import Iterator, MutableMapping
from concurrent.futures import Future
from datetime import datetime
from multiprocessing import Event, get_context
from multiprocessing.synchronize import Event as MultiprocessingEvent
from pathlib import Path
from typing import Any, Callable
from unittest import TestCase, mock
from uuid import uuid4

import grpc
import msgpack
import pytest
import zstandard as zstd
from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.kafka.producer import FutureTrackingProducer
from arroyo.backends.kafka.producer import _pending_futures as _arroyo_pending_futures
from arroyo.types import BrokerValue, Partition, Topic
from redis import StrictRedis

# from sentry.utils.redis import redis_clusters
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    ON_ATTEMPTS_EXCEEDED_DISCARD,
    TASK_ACTIVATION_STATUS_COMPLETE,
    TASK_ACTIVATION_STATUS_FAILURE,
    TASK_ACTIVATION_STATUS_RETRY,
    FetchNextTask,
    PushTaskRequest,
    PushTaskResponse,
    RetryState,
    TaskActivation,
)
from sentry_sdk.crons import MonitorStatus

from taskbroker_client.canary import CANARY_TASK_NAME
from taskbroker_client.constants import INTERNAL_NAMESPACE, CompressionType
from taskbroker_client.retry import NoRetriesRemainingError
from taskbroker_client.state import current_task
from taskbroker_client.types import InflightTaskActivation, ProcessingResult
from taskbroker_client.worker.worker import (
    PushTaskWorker,
    ShutdownSignal,
    TaskWorker,
    TaskWorkerProcessingPool,
    TrackedChild,
    WorkerServicer,
)
from taskbroker_client.worker.workerchild import ChildMessage
from taskbroker_client.worker.workerchild import child_process as _child_process

SIMPLE_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="111",
        taskname="examples.simple_task",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
    ),
)

CANARY_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="canary",
        taskname=CANARY_TASK_NAME,
        namespace=INTERNAL_NAMESPACE,
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
    ),
)

RETRY_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="222",
        taskname="examples.retry_task",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
    ),
)

FAIL_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="333",
        taskname="examples.fail_task",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
    ),
)

UNDEFINED_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="444",
        taskname="total.rubbish",
        namespace="lolnope",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
    ),
)

AT_MOST_ONCE_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="555",
        taskname="examples.at_most_once",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
    ),
)

RETRY_STATE_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="654",
        taskname="examples.retry_state",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
        retry_state=RetryState(
            # no more attempts left
            attempts=1,
            max_attempts=2,
            on_attempts_exceeded=ON_ATTEMPTS_EXCEEDED_DISCARD,
        ),
    ),
)

SCHEDULED_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="111",
        taskname="examples.simple_task",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
        headers={
            "sentry-monitor-slug": "simple-task",
            "sentry-monitor-check-in-id": "abc123",
        },
    ),
)

COMPRESSED_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="compressed_task_123",
        taskname="examples.simple_task",
        namespace="examples",
        parameters_bytes=zstd.compress(
            msgpack.packb(
                {
                    "args": ["test_arg1", "test_arg2"],
                    "kwargs": {"test_key": "test_value", "number": 42},
                },
                use_bin_type=True,
            )
        ),
        headers={
            "compression-type": CompressionType.ZSTD.value,
        },
        processing_deadline_duration=2,
    ),
)

# Task with Retry logic, expected exceptions to silence reporting
RETRY_TASK_WITH_SILENCED_TIMEOUT = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="654",
        taskname="examples.will_timeout_without_reporting",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=1,
        retry_state=RetryState(
            # no more attempts left
            attempts=1,
            max_attempts=2,
            on_attempts_exceeded=ON_ATTEMPTS_EXCEEDED_DISCARD,
        ),
    ),
)

# Task with Retry logic, expected exceptions to silence reporting
RETRY_TASK_WITH_SILENCED_UNHANDLED_EXCEPTION = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="654",
        taskname="examples.will_fail_with_silenced_exception",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
        retry_state=RetryState(
            # One retry left
            attempts=0,
            max_attempts=2,
            on_attempts_exceeded=ON_ATTEMPTS_EXCEEDED_DISCARD,
        ),
    ),
)

# Task set to retry on deadline exceeded exceptions
RETRY_TASK_WITH_SILENCED_IGNORED_EXCEPTION = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="654",
        taskname="examples.will_fail_with_silenced_ignored_exception",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=2,
        retry_state=RetryState(
            # One retry left
            attempts=0,
            max_attempts=2,
            on_attempts_exceeded=ON_ATTEMPTS_EXCEEDED_DISCARD,
        ),
    ),
)

# Task set to retry on deadline exceeded exceptions
RETRY_TASK_ON_DEADLINE_EXCEEDED = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="654",
        taskname="examples.will_retry_on_deadline_exceeded",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=1,
        retry_state=RetryState(
            # One retry left
            attempts=0,
            max_attempts=2,
            on_attempts_exceeded=ON_ATTEMPTS_EXCEEDED_DISCARD,
        ),
    ),
)

TASK_WITH_HEADERS = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="headers_task_123",
        taskname="examples.task_with_headers",
        namespace="examples",
        parameters_bytes=msgpack.packb({"args": ["test_value"], "kwargs": {}}, use_bin_type=True),
        headers={
            "x-custom-header": "custom_value",
            "sentry-trace": "trace-id",
        },
        processing_deadline_duration=2,
    ),
)


def _make_processing_result(task_id: str) -> ProcessingResult:
    return ProcessingResult(
        task_id=task_id,
        status=TASK_ACTIVATION_STATUS_COMPLETE,
        host="localhost:50051",
        receive_timestamp=0,
    )


def child_process(
    app_module: str,
    child_tasks: queue.Queue[InflightTaskActivation],
    processed_tasks: queue.Queue[ProcessingResult],
    shutdown_event: MultiprocessingEvent,
    max_task_count: int | None,
    processing_pool_name: str,
    process_type: str,
    skip_awaiting_futures: bool,
    future_checking_frequency: float,
) -> None:
    ctx = get_context("fork")
    messages = ctx.Queue()
    parent_release = ctx.Event()
    parent_release.set()

    _child_process(
        uuid4(),
        app_module,
        child_tasks,
        processed_tasks,
        shutdown_event,
        max_task_count,
        processing_pool_name,
        process_type,
        skip_awaiting_futures,
        future_checking_frequency,
        messages,
        parent_release,
    )


class _SendResultCapture:
    def __init__(self) -> None:
        self.send_calls: list[tuple[list[ProcessingResult], bool]] = []
        self._lock = threading.Lock()

    def __call__(self, results: list[ProcessingResult], is_draining: bool) -> None:
        with self._lock:
            self.send_calls.append((list(results), is_draining))
        return None

    def wait_for_calls(self, expected: int, timeout: float = 5) -> None:
        start = time.time()
        while len(self.send_calls) < expected and time.time() - start < timeout:
            time.sleep(0.01)
        if len(self.send_calls) < expected:
            raise AssertionError(f"Expected {expected} send calls, got {len(self.send_calls)}")


def _make_result_thread_pool(
    capture: _SendResultCapture,
    *,
    concurrency: int = 3,
    result_queue_maxsize: int = 3,
    update_in_batches: bool = False,
) -> TaskWorkerProcessingPool:
    return TaskWorkerProcessingPool(
        app_module="examples.app:app",
        send_result_fn=capture,
        mp_context=get_context("fork"),
        max_child_task_count=100,
        concurrency=concurrency,
        result_queue_maxsize=result_queue_maxsize,
        processing_pool_name="test",
        update_in_batches=update_in_batches,
        process_type="fork",
    )


class _FakeProcess:
    _next_pid = 1

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.name = kwargs["name"]
        self.target = kwargs["target"]
        self.args = kwargs["args"]
        self.pid = _FakeProcess._next_pid
        _FakeProcess._next_pid += 1
        self.exitcode: int | None = None
        self.started = False
        self.alive = False
        self.terminated = False
        self.killed = False
        self.join_calls: list[float | None] = []

    def start(self) -> None:
        self.started = True
        self.alive = True

    def is_alive(self) -> bool:
        return self.alive

    def join(self, timeout: float | None = None) -> None:
        self.join_calls.append(timeout)

    def terminate(self) -> None:
        self.terminated = True
        self.alive = False
        self.exitcode = -signal.SIGTERM

    def kill(self) -> None:
        self.killed = True
        self.alive = False
        self.exitcode = -signal.SIGKILL


class _FakeContext:
    def __init__(self) -> None:
        self.processes: list[_FakeProcess] = []
        self.queues: list[queue.Queue[Any]] = []

    def Queue(self, maxsize: int = 0) -> queue.Queue[Any]:
        created: queue.Queue[Any] = queue.Queue(maxsize=maxsize)
        self.queues.append(created)
        return created

    def Event(self) -> threading.Event:
        return threading.Event()

    def Process(self, *args: Any, **kwargs: Any) -> _FakeProcess:
        process = _FakeProcess(*args, **kwargs)
        self.processes.append(process)
        return process


def _make_fake_context_pool(
    fake_context: _FakeContext,
    *,
    concurrency: int = 1,
    min_concurrency: int = 0,
) -> TaskWorkerProcessingPool:
    return TaskWorkerProcessingPool(
        app_module="examples.app:app",
        send_result_fn=lambda x, y: None,
        mp_context=fake_context,  # type: ignore[arg-type]
        max_child_task_count=1,
        concurrency=concurrency,
        min_concurrency=min_concurrency,
        processing_pool_name="test",
        process_type="fork",
    )


def test_shutdown_signal_wait_times_out() -> None:
    shutdown_signal = ShutdownSignal()

    start = time.monotonic()
    assert shutdown_signal.wait(0.2) is False
    assert time.monotonic() - start >= 0.2


def test_shutdown_signal_wait_returns_immediately_when_requested() -> None:
    shutdown_signal = ShutdownSignal()
    shutdown_signal.request()

    start = time.monotonic()
    assert shutdown_signal.wait(30) is True
    assert time.monotonic() - start < 1


def test_shutdown_signal_request_does_not_touch_the_event() -> None:
    """
    `request()` must not take a lock, so it must not touch the event.

    This is the entire reason the class exists: it is the only method a signal
    handler may call, and `Event.set()` takes a non-reentrant lock that can
    deadlock against whatever the handler interrupted. A refactor that "helpfully"
    sets the event here would be silently unsafe, and the wakeup timings asserted
    below are too loose to notice.
    """
    shutdown_signal = ShutdownSignal()
    shutdown_signal.request()

    assert shutdown_signal.is_set() is True
    assert shutdown_signal._event.is_set() is False

    # ...whereas set() is allowed to, and does.
    shutdown_signal.set()
    assert shutdown_signal._event.is_set() is True


def test_shutdown_signal_wait_wakes_on_request_from_another_thread() -> None:
    shutdown_signal = ShutdownSignal()
    threading.Timer(0.1, shutdown_signal.request).start()

    start = time.monotonic()
    assert shutdown_signal.wait(30) is True
    # request() cannot wake the event, so this is the poll interval, not instant.
    assert time.monotonic() - start < 5


def test_shutdown_signal_wait_wakes_on_set_from_another_thread() -> None:
    shutdown_signal = ShutdownSignal()
    threading.Timer(0.1, shutdown_signal.set).start()

    start = time.monotonic()
    assert shutdown_signal.wait(30) is True
    assert time.monotonic() - start < 1


def test_shutdown_signal_request_from_signal_handler_during_wait() -> None:
    """
    A handler that fires while wait() is sleeping must not hang. Event.set()
    can deadlock here because wait() may already hold the event's lock.
    """
    shutdown_signal = ShutdownSignal()
    previous = signal.signal(signal.SIGALRM, lambda *args: shutdown_signal.request())
    try:
        signal.setitimer(signal.ITIMER_REAL, 0.1)
        start = time.monotonic()
        assert shutdown_signal.wait(30) is True
        assert time.monotonic() - start < 5
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _capture_signal_handlers(
    handlers: dict[int, Callable[..., None]],
) -> Callable[[int, Callable[..., None]], None]:
    """
    Stand-in for signal.signal that records handlers instead of installing them,
    so tests can deliver a signal without touching the pytest process.
    """

    def install_handler(signum: int, handler: Callable[..., None]) -> None:
        handlers[signum] = handler

    return install_handler


def _wait_for(condition: Callable[[], bool], timeout: float = 5) -> None:
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


class TestTaskWorker(TestCase):
    def test_fetch_task(self) -> None:
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=100,
            process_type="fork",
        )
        with mock.patch.object(taskworker.client, "get_task") as mock_get:
            mock_get.return_value = SIMPLE_TASK

            task = taskworker.fetch_task()
            mock_get.assert_called_once()

        assert task
        assert task.activation.id == SIMPLE_TASK.activation.id

    def test_fetch_task_skips_request_during_shutdown(self) -> None:
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=100,
            process_type="fork",
        )
        taskworker._shutdown_signal.request()

        with mock.patch.object(taskworker.client, "get_task") as mock_get:
            task = taskworker.fetch_task()

        assert task is None
        mock_get.assert_not_called()

    def test_fetch_task_drops_task_claimed_during_shutdown(self) -> None:
        """
        get_task() blocks with no deadline, so SIGTERM can land mid-RPC.

        Handing the activation to a child anyway claims work we will not run,
        which then has to expire on the broker before anyone else picks it up.
        """
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=100,
            process_type="fork",
        )

        def get_task(namespace: str | None = None) -> InflightTaskActivation:
            # Shutdown requested while the RPC was in flight.
            taskworker._shutdown_signal.request()
            return SIMPLE_TASK

        with mock.patch.object(taskworker.client, "get_task", side_effect=get_task) as mock_get:
            task = taskworker.fetch_task()

        mock_get.assert_called_once()
        assert task is None

    def test_send_update_task_does_not_fetch_next_during_shutdown(self) -> None:
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=100,
            process_type="fork",
        )
        taskworker._shutdown_signal.request()
        result = _make_processing_result("completed")

        with mock.patch.object(taskworker.client, "update_task", return_value=None) as mock_update:
            taskworker._send_update_task(result, FetchNextTask(namespace="examples"))

        mock_update.assert_called_once_with(result, None)

    def test_start_exits_cleanly_on_sigterm(self) -> None:
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=100,
            process_type="fork",
        )
        handlers: dict[int, Callable[..., None]] = {}

        def deliver_sigterm() -> None:
            handlers[signal.SIGTERM](signal.SIGTERM, None)

        with (
            mock.patch(
                "taskbroker_client.worker.worker.signal.signal",
                side_effect=_capture_signal_handlers(handlers),
            ),
            mock.patch.object(taskworker.worker_pool, "start_metrics_thread"),
            mock.patch.object(taskworker.worker_pool, "start_result_thread"),
            mock.patch.object(taskworker.worker_pool, "start_spawn_children_thread"),
            mock.patch.object(taskworker.worker_pool, "shutdown") as pool_shutdown,
            mock.patch.object(taskworker, "run_once", side_effect=deliver_sigterm) as run_once,
        ):
            exitcode = taskworker.start()

        # The handler returns instead of raising, so the loop finishes the
        # iteration it was in and then exits.
        assert exitcode == 0
        assert run_once.call_count == 1
        assert taskworker._shutdown_signal.is_set()
        pool_shutdown.assert_called_once_with()

    def test_fetch_no_task(self) -> None:
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=100,
            process_type="fork",
        )
        with mock.patch.object(taskworker.client, "get_task") as mock_get:
            mock_get.return_value = None
            task = taskworker.fetch_task()

            mock_get.assert_called_once()
        assert task is None

    def test_run_once_no_next_task(self) -> None:
        max_runtime = 5
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=1,
            process_type="fork",
        )
        with mock.patch.object(taskworker, "client") as mock_client:
            mock_client.get_task.return_value = SIMPLE_TASK
            # No next_task returned
            mock_client.update_task.return_value = None

            taskworker.worker_pool.start_result_thread()
            taskworker.worker_pool.start_spawn_children_thread()
            start = time.time()
            while True:
                taskworker.run_once()
                if mock_client.update_task.called:
                    break
                if time.time() - start > max_runtime:
                    taskworker.shutdown()
                    raise AssertionError("Timeout waiting for update_task to be called")

            taskworker.shutdown()
            assert mock_client.get_task.called
            assert mock_client.update_task.call_count >= 1
            first_update = mock_client.update_task.call_args_list[0]
            assert first_update.args[0].host == "localhost:50051"
            assert first_update.args[0].task_id == SIMPLE_TASK.activation.id
            assert first_update.args[0].status == TASK_ACTIVATION_STATUS_COMPLETE

    def test_run_once_with_next_task(self) -> None:
        # Cover the scenario where update_task returns the next task which should
        # be processed.
        max_runtime = 5
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=1,
            process_type="fork",
        )
        with mock.patch.object(taskworker, "client") as mock_client:

            def update_task_response(*args: Any, **kwargs: Any) -> InflightTaskActivation | None:
                if mock_client.update_task.call_count >= 1:
                    return None
                return SIMPLE_TASK

            mock_client.update_task.side_effect = update_task_response
            mock_client.get_task.return_value = SIMPLE_TASK
            taskworker.worker_pool.start_result_thread()
            taskworker.worker_pool.start_spawn_children_thread()

            # Run until two tasks have been processed
            start = time.time()
            while True:
                taskworker.run_once()
                if mock_client.update_task.call_count >= 2:
                    break
                if time.time() - start > max_runtime:
                    taskworker.shutdown()
                    raise AssertionError("Timeout waiting for get_task to be called")

            taskworker.shutdown()
            assert mock_client.get_task.called
            assert mock_client.update_task.call_count >= 2
            for update_call in mock_client.update_task.call_args_list[:2]:
                assert update_call.args[0].host == "localhost:50051"
                assert update_call.args[0].task_id == SIMPLE_TASK.activation.id
                assert update_call.args[0].status == TASK_ACTIVATION_STATUS_COMPLETE

    def test_run_once_with_update_failure(self) -> None:
        # Cover the scenario where update_task fails a few times in a row
        # We should retain the result until RPC succeeds.
        max_runtime = 5
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=1,
            process_type="fork",
        )
        with mock.patch.object(taskworker, "client") as mock_client:

            def update_task_response(*args: Any, **kwargs: Any) -> None:
                if mock_client.update_task.call_count <= 2:
                    # Use setattr() because internally grpc uses _InactiveRpcError
                    # but it isn't exported.
                    err = grpc.RpcError("update task failed")
                    setattr(err, "code", lambda: grpc.StatusCode.UNAVAILABLE)
                    raise err
                return None

            def get_task_response(*args: Any, **kwargs: Any) -> InflightTaskActivation | None:
                # Only one task that fails to update
                if mock_client.get_task.call_count == 1:
                    return SIMPLE_TASK
                return None

            mock_client.update_task.side_effect = update_task_response
            mock_client.get_task.side_effect = get_task_response
            taskworker.worker_pool.start_result_thread()
            taskworker.worker_pool.start_spawn_children_thread()

            # Run until the update has 'completed'
            start = time.time()
            while True:
                taskworker.run_once()
                if mock_client.update_task.call_count >= 3:
                    break
                if time.time() - start > max_runtime:
                    taskworker.shutdown()
                    raise AssertionError("Timeout waiting for get_task to be called")

            taskworker.shutdown()
            assert mock_client.get_task.called
            assert mock_client.update_task.call_count == 3

    def test_push_task_queue(self) -> None:
        taskworker = TaskWorkerProcessingPool(
            app_module="examples.app:app",
            send_result_fn=lambda x, y: None,
            mp_context=get_context("fork"),
            max_child_task_count=100,
            concurrency=1,
            child_tasks_queue_maxsize=2,
            result_queue_maxsize=2,
            processing_pool_name="test",
            process_type="fork",
        )

        # We can enqueue the first task
        result = taskworker.push_task(SIMPLE_TASK, timeout=None)
        self.assertTrue(result)

        # We can enqueue the second task
        result = taskworker.push_task(SIMPLE_TASK, timeout=1)
        self.assertTrue(result)

        # We cannot enqueue the third task because the queue is full
        result = taskworker.push_task(SIMPLE_TASK, timeout=1)
        self.assertFalse(result)

    def test_result_thread_sends_full_batch(self) -> None:
        capture = _SendResultCapture()
        concurrency = 3
        pool = _make_result_thread_pool(capture, concurrency=concurrency, update_in_batches=True)
        try:
            pool.start_result_thread()

            for i in range(concurrency):
                pool.put_result(_make_processing_result(str(i)))

            capture.wait_for_calls(1)
            batch, is_draining = capture.send_calls[0]
            self.assertEqual(len(batch), concurrency)
            self.assertEqual({result.task_id for result in batch}, {"0", "1", "2"})
            self.assertFalse(is_draining)
        finally:
            pool.shutdown()

    def test_result_thread_flushes_partial_batch_on_queue_empty(self) -> None:
        capture = _SendResultCapture()
        pool = _make_result_thread_pool(capture, update_in_batches=True)
        try:
            pool.start_result_thread()

            pool.put_result(_make_processing_result("partial-1"))
            pool.put_result(_make_processing_result("partial-2"))

            capture.wait_for_calls(1, timeout=3)
            batch, is_draining = capture.send_calls[0]
            self.assertEqual(len(batch), 2)
            self.assertEqual({result.task_id for result in batch}, {"partial-1", "partial-2"})
            self.assertFalse(is_draining)
        finally:
            pool.shutdown()

    def test_result_thread_sends_results_individually_without_batching(self) -> None:
        capture = _SendResultCapture()
        pool = _make_result_thread_pool(capture)
        try:
            pool.start_result_thread()

            pool.put_result(_make_processing_result("single"))

            capture.wait_for_calls(1)
            batch, is_draining = capture.send_calls[0]
            self.assertEqual(len(batch), 1)
            self.assertEqual(batch[0].task_id, "single")
            self.assertFalse(is_draining)
        finally:
            pool.shutdown()

    def test_run_once_current_task_state(self) -> None:
        # Run a task that uses retry_task() helper
        # to raise and catch a NoRetriesRemainingError
        max_runtime = 5
        taskworker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=["127.0.0.1:50051"],
            max_child_task_count=1,
            process_type="fork",
        )
        with mock.patch.object(taskworker, "client") as mock_client:

            def update_task_response(*args: Any, **kwargs: Any) -> None:
                return None

            mock_client.update_task.side_effect = update_task_response
            mock_client.get_task.return_value = RETRY_STATE_TASK
            taskworker.worker_pool.start_result_thread()
            taskworker.worker_pool.start_spawn_children_thread()

            # Run until two tasks have been processed
            start = time.time()
            while True:
                taskworker.run_once()
                time.sleep(0.1)
                if mock_client.update_task.call_count >= 1:
                    break
                if time.time() - start > max_runtime:
                    taskworker.shutdown()
                    raise AssertionError("Timeout waiting for update_task to be called")

            taskworker.shutdown()
            assert mock_client.get_task.called
            assert mock_client.update_task.call_count == 1
            # status is complete, as retry_state task handles the NoRetriesRemainingError
            assert mock_client.update_task.call_args.args[0].host == "localhost:50051"
            assert (
                mock_client.update_task.call_args.args[0].task_id == RETRY_STATE_TASK.activation.id
            )
            assert (
                mock_client.update_task.call_args.args[0].status == TASK_ACTIVATION_STATUS_COMPLETE
            )

            # TODO read host from env vars
            redis = StrictRedis(host="localhost", port=6379, decode_responses=True)
            assert current_task() is None, "should clear current task on completion"
            assert redis.get("no-retries-remaining"), "key should exist if except block was hit"
            redis.delete("no-retries-remaining")

    def test_constructor_push_mode(self) -> None:
        taskworker = PushTaskWorker(
            app_module="examples.app:app",
            broker_service="127.0.0.1:50051",
            max_child_task_count=100,
            process_type="fork",
            grpc_port=50099,
        )

        self.assertTrue(taskworker.client is not None)
        self.assertEqual(taskworker._grpc_port, 50099)


def test_pull_worker_health_check_touches_while_full(tmp_path: Path) -> None:
    health_check_path = tmp_path / "health"
    taskworker = TaskWorker(
        app_module="examples.app:app",
        broker_hosts=["127.0.0.1:50051"],
        max_child_task_count=100,
        process_type="fork",
        health_check_file_path=str(health_check_path),
        health_check_sec_per_touch=1,
    )

    with (
        mock.patch.object(taskworker.worker_pool, "is_worker_full", return_value=True),
        mock.patch.object(taskworker.client, "get_task") as mock_get_task,
        mock.patch("taskbroker_client.worker.worker.time.sleep"),
        mock.patch("taskbroker_client.worker.client.time") as mock_time,
    ):
        mock_time.time.return_value = 1
        taskworker.run_once()
        assert health_check_path.exists()

        health_check_path.unlink()
        mock_time.time.return_value = 3
        taskworker.run_once()

    assert health_check_path.exists()
    mock_get_task.assert_not_called()


def test_push_worker_health_check_touches_while_idle(tmp_path: Path) -> None:
    taskworker = PushTaskWorker(
        app_module="examples.app:app",
        broker_service="127.0.0.1:50051",
        max_child_task_count=100,
        process_type="fork",
        health_check_file_path=str(tmp_path / "health"),
        health_check_sec_per_touch=0.01,
    )

    with mock.patch.object(taskworker.client, "emit_health_check") as mock_emit:
        taskworker._start_health_check_thread()
        try:
            start = time.time()
            while mock_emit.call_count < 2 and time.time() - start < 1:
                time.sleep(0.01)
        finally:
            taskworker._stop_health_check_thread()

    assert mock_emit.call_count >= 2
    assert taskworker._health_check_thread is None


def _make_push_worker(**kwargs: Any) -> PushTaskWorker:
    return PushTaskWorker(
        app_module="examples.app:app",
        broker_service="127.0.0.1:50051",
        max_child_task_count=100,
        process_type="fork",
        **kwargs,
    )


def test_await_children_warm_returns_when_ready() -> None:
    taskworker = _make_push_worker(concurrency=4, warmup_timeout=5)

    with (
        mock.patch.object(taskworker, "_metrics") as mock_metrics,
        mock.patch.object(
            TaskWorkerProcessingPool, "ready_count", new_callable=mock.PropertyMock
        ) as ready_count,
    ):
        ready_count.return_value = 4
        start = time.time()
        taskworker._await_children_warm()
        elapsed = time.time() - start

    assert elapsed < 1
    # Records warmup duration, but no timeout.
    timeout_calls = [
        c
        for c in mock_metrics.incr.call_args_list
        if c.args[0] == "taskworker.worker.warmup_timeout"
    ]
    assert timeout_calls == []
    mock_metrics.distribution.assert_any_call(
        "taskworker.worker.warmup_duration", mock.ANY, tags=mock.ANY
    )


def test_await_children_warm_times_out() -> None:
    taskworker = _make_push_worker(concurrency=4, warmup_timeout=0.1)

    with mock.patch.object(taskworker, "_metrics") as mock_metrics:
        start = time.time()
        taskworker._await_children_warm()
        elapsed = time.time() - start

    assert elapsed >= 0.1
    mock_metrics.incr.assert_any_call(
        "taskworker.worker.warmup_timeout", tags={"processing_pool": "unknown"}
    )


def test_await_children_warm_unblocks_when_children_warm() -> None:
    taskworker = _make_push_worker(concurrency=2, warmup_timeout=5)
    ready_child_count = 0

    def warm_up() -> None:
        nonlocal ready_child_count
        time.sleep(0.2)
        ready_child_count = 2

    warmer = threading.Thread(target=warm_up)
    warmer.start()
    try:
        with (
            mock.patch.object(taskworker, "_metrics") as mock_metrics,
            mock.patch.object(
                TaskWorkerProcessingPool, "ready_count", new_callable=mock.PropertyMock
            ) as ready_count,
        ):
            ready_count.side_effect = lambda: ready_child_count
            start = time.time()
            taskworker._await_children_warm()
            elapsed = time.time() - start
    finally:
        warmer.join()

    assert 0.2 <= elapsed < 5
    timeout_calls = [
        c
        for c in mock_metrics.incr.call_args_list
        if c.args[0] == "taskworker.worker.warmup_timeout"
    ]
    assert timeout_calls == []


@contextlib.contextmanager
def _push_worker_grpc_mocks(
    taskworker: PushTaskWorker,
    fake_server: mock.MagicMock,
    fake_health: mock.MagicMock,
    handlers: dict[int, Callable[..., None]] | None = None,
) -> Iterator[mock.MagicMock]:
    """
    Patch out everything PushTaskWorker.start() touches apart from its own
    shutdown handling. Yields the mocked pool shutdown.
    """
    with contextlib.ExitStack() as stack:
        if handlers is not None:
            stack.enter_context(
                mock.patch(
                    "taskbroker_client.worker.worker.signal.signal",
                    side_effect=_capture_signal_handlers(handlers),
                )
            )
        for name in (
            "start_metrics_thread",
            "start_result_thread",
            "start_spawn_children_thread",
        ):
            stack.enter_context(mock.patch.object(taskworker.worker_pool, name))
        pool_shutdown = stack.enter_context(mock.patch.object(taskworker.worker_pool, "shutdown"))
        stack.enter_context(mock.patch.object(taskworker, "_start_health_check_thread"))
        stack.enter_context(mock.patch.object(taskworker, "_stop_health_check_thread"))
        stack.enter_context(
            mock.patch("taskbroker_client.worker.worker.grpc.server", return_value=fake_server)
        )
        stack.enter_context(
            mock.patch(
                "taskbroker_client.worker.worker.health.HealthServicer", return_value=fake_health
            )
        )
        stack.enter_context(
            mock.patch(
                "taskbroker_client.worker.worker.health_pb2_grpc.add_HealthServicer_to_server"
            )
        )
        stack.enter_context(
            mock.patch(
                "taskbroker_client.worker.worker.taskbroker_pb2_grpc"
                ".add_WorkerServiceServicer_to_server"
            )
        )
        yield pool_shutdown


def test_push_start_exits_cleanly_on_sigterm() -> None:
    taskworker = _make_push_worker(concurrency=2, warmup_timeout=5)
    handlers: dict[int, Callable[..., None]] = {}

    fake_health = mock.MagicMock()
    fake_server = mock.MagicMock()

    # Deliver the signal from inside the poll, which is where a real SIGTERM
    # would land once the server is up and serving. True keeps the server
    # "healthy", so only the flipped bool can end the loop.
    #
    # Deliberately not on the first poll: a loop that exits after one iteration
    # no matter what its condition says would pass either way, which is exactly
    # how the inverted-boolean bug got through review. Surviving to poll 3 means
    # the exit condition is actually being exercised.
    calls = 0

    def wait_for_termination(timeout: float | None = None) -> bool:
        nonlocal calls
        calls += 1
        if calls == 3:
            handlers[signal.SIGTERM](signal.SIGTERM, None)
        return True

    fake_server.wait_for_termination.side_effect = wait_for_termination

    with (
        _push_worker_grpc_mocks(taskworker, fake_server, fake_health, handlers) as pool_shutdown,
        mock.patch.object(
            TaskWorkerProcessingPool, "ready_count", new_callable=mock.PropertyMock, return_value=2
        ),
    ):
        exitcode = taskworker.start()

    assert exitcode == 0
    assert taskworker._shutdown_signal.is_set()
    # The handler only flipped a bool, so we polled our way out rather than
    # relying on the server being stopped from inside the handler. Three polls
    # means we kept serving until the signal, then left promptly.
    assert calls == 3
    fake_server.stop.assert_called_once_with(grace=5)
    pool_shutdown.assert_called_once_with()


def test_push_start_keeps_serving_while_server_is_healthy() -> None:
    """
    A healthy server must not end the serve loop.

    `grpc.Server.wait_for_termination(timeout=...)` returns True when the
    timeout elapsed, i.e. while the server is still up, and False once it has
    terminated -- the inverse of `Event.wait()`. A previous version of this
    patch treated that True as "terminated" and so exited one poll interval
    after startup, taking down every worker. Guard against reintroducing it by
    making the mock behave the way grpc really does.
    """
    taskworker = _make_push_worker(concurrency=2, warmup_timeout=5)
    handlers: dict[int, Callable[..., None]] = {}

    fake_health = mock.MagicMock()
    fake_server = mock.MagicMock()

    polls = 0

    def wait_for_termination(timeout: float | None = None) -> bool:
        nonlocal polls
        polls += 1
        # Survive several polls, then SIGTERM so the test terminates.
        if polls >= 5:
            handlers[signal.SIGTERM](signal.SIGTERM, None)
        # Healthy server: the timeout is always what elapses.
        return True

    fake_server.wait_for_termination.side_effect = wait_for_termination

    with (
        _push_worker_grpc_mocks(taskworker, fake_server, fake_health, handlers) as pool_shutdown,
        mock.patch.object(
            TaskWorkerProcessingPool, "ready_count", new_callable=mock.PropertyMock, return_value=2
        ),
    ):
        exitcode = taskworker.start()

    assert exitcode == 0
    # We kept looping instead of bailing out on the first poll.
    assert polls == 5
    fake_server.stop.assert_called_once_with(grace=5)
    pool_shutdown.assert_called_once_with()


def test_push_start_exits_when_server_terminates_unexpectedly() -> None:
    """
    A server that goes away on its own must end the serve loop.

    Otherwise the parent keeps its children alive and keeps touching the health
    check file while no longer accepting tasks.
    """
    taskworker = _make_push_worker(concurrency=2, warmup_timeout=5)
    handlers: dict[int, Callable[..., None]] = {}

    fake_health = mock.MagicMock()
    fake_server = mock.MagicMock()

    # False means the server terminated, without anyone asking it to. Bail out
    # after a few polls so a loop that ignores this fails the assertion below
    # instead of hanging until CI times out.
    polls = 0

    def wait_for_termination(timeout: float | None = None) -> bool:
        nonlocal polls
        polls += 1
        if polls > 3:
            raise AssertionError("serve loop ignored a terminated server")
        return False

    fake_server.wait_for_termination.side_effect = wait_for_termination

    with (
        _push_worker_grpc_mocks(taskworker, fake_server, fake_health, handlers) as pool_shutdown,
        mock.patch.object(
            TaskWorkerProcessingPool, "ready_count", new_callable=mock.PropertyMock, return_value=2
        ),
    ):
        exitcode = taskworker.start()

    assert exitcode == 0
    # Noticed on the first poll, rather than spinning forever.
    assert polls == 1
    pool_shutdown.assert_called_once_with()


def test_push_start_does_not_start_server_when_shutdown_requested_first() -> None:
    taskworker = _make_push_worker(concurrency=2, warmup_timeout=5)
    taskworker._shutdown_signal.request()

    fake_health = mock.MagicMock()
    fake_server = mock.MagicMock()

    with _push_worker_grpc_mocks(taskworker, fake_server, fake_health) as pool_shutdown:
        exitcode = taskworker.start()

    assert exitcode == 0
    fake_server.start.assert_not_called()
    # Stopping a server that was never started is not something grpc promises
    # to handle.
    fake_server.stop.assert_not_called()
    fake_server.wait_for_termination.assert_not_called()
    pool_shutdown.assert_called_once_with()


def test_push_start_does_not_serve_when_shutdown_during_warmup() -> None:
    from grpc_health.v1 import health_pb2

    taskworker = _make_push_worker(concurrency=2, warmup_timeout=5)
    handlers: dict[int, Callable[..., None]] = {}

    fake_health = mock.MagicMock()
    fake_server = mock.MagicMock()

    # Children never warm; SIGTERM arrives while we are waiting on them.
    def ready_count() -> int:
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return 0

    with (
        _push_worker_grpc_mocks(taskworker, fake_server, fake_health, handlers) as pool_shutdown,
        mock.patch.object(
            TaskWorkerProcessingPool,
            "ready_count",
            new_callable=mock.PropertyMock,
            side_effect=ready_count,
        ),
    ):
        exitcode = taskworker.start()

    assert exitcode == 0
    # Health must never have been flipped to SERVING.
    serving_calls = [
        c
        for c in fake_health.set.call_args_list
        if c.args[1] == health_pb2.HealthCheckResponse.SERVING
    ]
    assert serving_calls == []
    fake_server.start.assert_called_once()
    fake_server.stop.assert_called_once_with(grace=5)
    fake_server.wait_for_termination.assert_not_called()
    pool_shutdown.assert_called_once_with()


def _make_tracked_child(
    state: str,
    *,
    busy_since: float | None = None,
    busy_accumulated: float = 0.0,
    wait_since: float | None = None,
    wait_accumulated: float = 0.0,
) -> TrackedChild:
    return TrackedChild(
        process=mock.Mock(),
        state=state,  # type: ignore[arg-type]
        release=mock.Mock(),
        busy_since=busy_since,
        busy_accumulated=busy_accumulated,
        wait_since=wait_since,
        wait_accumulated=wait_accumulated,
    )


def _distribution_calls(metrics: mock.Mock, name: str) -> list[Any]:
    return [c for c in metrics.distribution.call_args_list if c.args[0] == name]


def _gauge_calls(metrics: mock.Mock, name: str) -> list[Any]:
    return [c for c in metrics.gauge.call_args_list if c.args[0] == name]


def test_emit_periodic_metrics_skips_occupancy_during_warmup() -> None:
    pool = _make_result_thread_pool(_SendResultCapture(), concurrency=4)
    pool._metrics = mock.Mock()

    # A freshly started pod has only pending children; none are consuming yet.
    with pool._children_lock:
        pool._children[uuid4()] = _make_tracked_child("pending")
        pool._children[uuid4()] = _make_tracked_child("pending")

    pool._emit_periodic_metrics()

    # Occupancy must not be emitted while warming up, otherwise fresh pods
    # publish misleading zeros that drag down the fleet-wide average.
    assert _gauge_calls(pool._metrics, "taskworker.worker.occupancy") == []
    # Other gauges still fire so warmup stays observable.
    assert _gauge_calls(pool._metrics, "taskworker.worker.children")
    # Concurrency is static and emitted even before any child is warm.
    concurrency_calls = _gauge_calls(pool._metrics, "taskworker.worker.concurrency")
    assert len(concurrency_calls) == 1
    assert concurrency_calls[0].args[1] == pytest.approx(4.0)


def test_emit_periodic_metrics_time_weights_busy_over_the_interval() -> None:
    # Worked example: interval [10.0, 11.0], 3 running children.
    #   A: 0.19s banked, idle at flush
    #   B: 0.30s banked + open segment since 10.70 -> +0.30 across the boundary
    #   C: 0.45s banked, idle at flush
    # busy_time = 0.19 + 0.60 + 0.45 = 1.24 -> occupancy = 1.24 / (1.0 * 3)
    pool = _make_result_thread_pool(_SendResultCapture(), concurrency=8)
    pool._metrics = mock.Mock()
    pool._last_occupancy_flush_at = 10.0

    child_b = uuid4()
    with pool._children_lock:
        pool._children[uuid4()] = _make_tracked_child("running", busy_accumulated=0.19)
        pool._children[child_b] = _make_tracked_child(
            "running", busy_accumulated=0.30, busy_since=10.70
        )
        pool._children[uuid4()] = _make_tracked_child("running", busy_accumulated=0.45)

    with mock.patch("taskbroker_client.worker.worker.time.monotonic", return_value=11.0):
        pool._emit_periodic_metrics()

    occupancy_calls = _gauge_calls(pool._metrics, "taskworker.worker.occupancy")
    assert len(occupancy_calls) == 1
    assert occupancy_calls[0].args[1] == pytest.approx(1.24 / 3)

    # The open segment is carried into the next interval; banks are drained.
    assert pool._children[child_b].busy_since == pytest.approx(11.0)
    for child in pool._children.values():
        assert child.busy_accumulated == 0.0
    assert pool._last_occupancy_flush_at == pytest.approx(11.0)


def test_emit_periodic_metrics_divides_by_running_children() -> None:
    # Two children busy for the whole 1s interval, one idle, one still warming.
    pool = _make_result_thread_pool(_SendResultCapture(), concurrency=8)
    pool._metrics = mock.Mock()
    pool._last_occupancy_flush_at = 10.0

    with pool._children_lock:
        pool._children[uuid4()] = _make_tracked_child("running", busy_accumulated=1.0)
        pool._children[uuid4()] = _make_tracked_child("running", busy_accumulated=1.0)
        pool._children[uuid4()] = _make_tracked_child("running", busy_accumulated=0.0)
        # Excluded from both numerator and denominator.
        pool._children[uuid4()] = _make_tracked_child("pending")

    with mock.patch("taskbroker_client.worker.worker.time.monotonic", return_value=11.0):
        pool._emit_periodic_metrics()

    occupancy_calls = _gauge_calls(pool._metrics, "taskworker.worker.occupancy")
    assert len(occupancy_calls) == 1
    # 2 busy-child-seconds over 3 running slots for a 1s interval.
    assert occupancy_calls[0].args[1] == pytest.approx(2 / 3)


def test_emit_periodic_metrics_clamps_occupancy_to_one() -> None:
    # A draining child can still be mid-task, so busy-time can exceed the running
    # capacity for the interval; occupancy must clamp to 1.0.
    pool = _make_result_thread_pool(_SendResultCapture(), concurrency=4)
    pool._metrics = mock.Mock()
    pool._last_occupancy_flush_at = 10.0

    with pool._children_lock:
        pool._children[uuid4()] = _make_tracked_child("running", busy_accumulated=1.0)
        pool._children[uuid4()] = _make_tracked_child("exiting", busy_accumulated=1.0)

    with mock.patch("taskbroker_client.worker.worker.time.monotonic", return_value=11.0):
        pool._emit_periodic_metrics()

    occupancy_calls = _gauge_calls(pool._metrics, "taskworker.worker.occupancy")
    assert len(occupancy_calls) == 1
    assert occupancy_calls[0].args[1] == pytest.approx(1.0)


def test_spawn_children_tracks_busy_and_idle_transitions() -> None:
    fake_context = _FakeContext()
    pool = _make_fake_context_pool(fake_context, concurrency=1)

    pool.start_spawn_children_thread()
    try:
        _wait_for(lambda: len(fake_context.processes) == 1)
        messages = fake_context.queues[-1]
        child_id = fake_context.processes[0].args[0]

        messages.put(ChildMessage(child_id, "running"))
        _wait_for(lambda: pool.ready_count == 1)

        # "busy" opens a segment.
        messages.put(ChildMessage(child_id, "busy"))
        _wait_for(lambda: pool._children[child_id].busy_since is not None)

        # "idle" closes it and banks a positive amount of busy-time.
        messages.put(ChildMessage(child_id, "idle"))
        _wait_for(
            lambda: pool._children[child_id].busy_since is None
            and pool._children[child_id].busy_accumulated > 0
        )
    finally:
        pool.shutdown()


def test_tracked_child_records_real_widths_for_events_in_one_batch() -> None:
    # The regression this change exists for: stamping at drain time collapsed
    # every segment in a batch to zero width. Two 50ms tasks, 10ms apart:
    child = _make_tracked_child("running", wait_since=100.00)

    child.mark_busy(100.00)
    child.mark_idle(100.05)
    child.mark_busy(100.06)
    child.mark_idle(100.11)

    # 0.05 + 0.05 of work, and the 0.01 gap between them counted as waiting.
    assert child.busy_accumulated == pytest.approx(0.10)
    assert child.wait_accumulated == pytest.approx(0.01)


def test_tracked_child_busy_and_wait_partition_the_interval() -> None:
    # A running child is always in exactly one state, so the drains must sum
    # to the interval width.
    child = _make_tracked_child("running", wait_since=10.0)

    child.mark_busy(10.4)
    busy = child.drain_busy(11.0)
    wait = child.drain_wait(11.0)

    assert busy == pytest.approx(0.6)
    assert wait == pytest.approx(0.4)
    assert busy + wait == pytest.approx(1.0)

    # Both open segments are carried forward rather than restarted at zero.
    assert child.busy_since == pytest.approx(11.0)
    assert child.wait_since is None


def test_tracked_child_ignores_events_older_than_the_last_drain() -> None:
    # drain_busy folds forward to the parent's clock, so a message stamped just
    # before that and processed just after must not subtract credited time.
    child = _make_tracked_child("running", busy_since=10.0)

    child.drain_busy(11.0)  # credits 1.0s, advances busy_since to 11.0
    child.mark_idle(10.95)  # stamped before the drain, delivered after

    assert child.busy_accumulated == pytest.approx(0.0)


def test_tracked_child_stops_accruing_wait_once_released() -> None:
    # A released child stops sending messages, so an open wait segment would
    # fold forward forever and make a recycling pool look starved.
    child = _make_tracked_child("running", wait_since=10.0)

    child.mark_stopped(10.5)
    assert child.drain_wait(20.0) == pytest.approx(0.5)
    assert child.drain_wait(30.0) == pytest.approx(0.0)


def test_tracked_child_pending_accrues_neither_busy_nor_wait() -> None:
    # Warmup is not starvation: a child importing the app has no slot to fill.
    child = _make_tracked_child("pending")

    assert child.drain_busy(11.0) == pytest.approx(0.0)
    assert child.drain_wait(11.0) == pytest.approx(0.0)

    child.mark_running(11.0)
    assert child.drain_wait(12.0) == pytest.approx(1.0)


def test_emit_periodic_metrics_emits_busy_and_wait_seconds() -> None:
    # Interval [10.0, 11.0]: one child busy throughout, one waiting 0.25s.
    pool = _make_result_thread_pool(_SendResultCapture(), concurrency=4)
    pool._metrics = mock.Mock()
    pool._last_occupancy_flush_at = 10.0

    with pool._children_lock:
        pool._children[uuid4()] = _make_tracked_child("running", busy_since=10.0)
        pool._children[uuid4()] = _make_tracked_child(
            "running", busy_accumulated=0.75, wait_since=10.75
        )

    with mock.patch("taskbroker_client.worker.worker.time.monotonic", return_value=11.0):
        pool._emit_periodic_metrics()

    busy = _distribution_calls(pool._metrics, "taskworker.worker.child_busy_seconds")
    wait = _distribution_calls(pool._metrics, "taskworker.worker.child_wait_seconds")
    assert len(busy) == 1 and len(wait) == 1
    assert busy[0].args[1] == pytest.approx(1.75)
    assert wait[0].args[1] == pytest.approx(0.25)

    # The scaler divides the pair, recovering occupancy without needing the
    # flush interval or the running-child count.
    assert busy[0].args[1] / (busy[0].args[1] + wait[0].args[1]) == pytest.approx(0.875)
    occupancy_calls = _gauge_calls(pool._metrics, "taskworker.worker.occupancy")
    assert occupancy_calls[0].args[1] == pytest.approx(1.75 / 2)


def test_emit_periodic_metrics_emits_counters_during_warmup() -> None:
    # Emitted with no running children, unlike occupancy: zero is correct for a
    # counter and separates "idle" from "not reporting".
    pool = _make_result_thread_pool(_SendResultCapture(), concurrency=4)
    pool._metrics = mock.Mock()

    with pool._children_lock:
        pool._children[uuid4()] = _make_tracked_child("pending")

    pool._emit_periodic_metrics()

    assert _gauge_calls(pool._metrics, "taskworker.worker.occupancy") == []
    assert len(_distribution_calls(pool._metrics, "taskworker.worker.child_busy_seconds")) == 1
    assert len(_distribution_calls(pool._metrics, "taskworker.worker.child_wait_seconds")) == 1


def test_spawn_children_uses_the_child_timestamp_not_the_drain_time() -> None:
    fake_context = _FakeContext()
    pool = _make_fake_context_pool(fake_context, concurrency=1)

    pool.start_spawn_children_thread()
    try:
        _wait_for(lambda: len(fake_context.processes) == 1)
        messages = fake_context.queues[-1]
        child_id = fake_context.processes[0].args[0]

        messages.put(ChildMessage(child_id, "running"))
        _wait_for(lambda: pool.ready_count == 1)

        stamped_at = time.monotonic() - 5.0
        messages.put(ChildMessage(child_id, "busy", timestamp=stamped_at))
        _wait_for(lambda: pool._children[child_id].busy_since is not None)

        # The drain lands up to 100ms later on another thread; the segment has
        # to start when the child said it did.
        assert pool._children[child_id].busy_since == pytest.approx(stamped_at)
    finally:
        pool.shutdown()


def test_spawn_children_tracks_wait_between_tasks() -> None:
    fake_context = _FakeContext()
    pool = _make_fake_context_pool(fake_context, concurrency=1)

    pool.start_spawn_children_thread()
    try:
        _wait_for(lambda: len(fake_context.processes) == 1)
        messages = fake_context.queues[-1]
        child_id = fake_context.processes[0].args[0]

        base = time.monotonic() - 10.0

        # Reporting in opens a wait segment: available, blocked in get().
        messages.put(ChildMessage(child_id, "running", timestamp=base))
        _wait_for(lambda: pool._children[child_id].wait_since == pytest.approx(base))

        # 2s of waiting, then 1s of work, then waiting again.
        messages.put(ChildMessage(child_id, "busy", timestamp=base + 2.0))
        messages.put(ChildMessage(child_id, "idle", timestamp=base + 3.0))
        # `wait_since` is already set by "running" above, so wait on banked busy.
        _wait_for(lambda: pool._children[child_id].busy_accumulated > 0)

        child = pool._children[child_id]
        assert child.wait_accumulated == pytest.approx(2.0)
        assert child.busy_accumulated == pytest.approx(1.0)
        assert child.busy_since is None
        assert child.wait_since == pytest.approx(base + 3.0)
    finally:
        pool.shutdown()


def test_spawn_children_counts_pending_children_toward_concurrency() -> None:
    fake_context = _FakeContext()
    pool = _make_fake_context_pool(fake_context, concurrency=2)

    pool.start_spawn_children_thread()
    try:
        _wait_for(lambda: len(fake_context.processes) == 2)
        time.sleep(0.25)

        assert len(fake_context.processes) == 2
        assert pool.ready_count == 0
    finally:
        pool.shutdown()


def test_spawn_children_releases_draining_child_above_min_concurrency() -> None:
    fake_context = _FakeContext()
    pool = _make_fake_context_pool(fake_context, concurrency=2, min_concurrency=1)

    pool.start_spawn_children_thread()
    try:
        _wait_for(lambda: len(fake_context.processes) == 2)
        messages = fake_context.queues[-1]
        first_process = fake_context.processes[0]
        first_child_id = first_process.args[0]
        first_release = first_process.args[-1]

        messages.put(ChildMessage(first_child_id, "running"))
        second_process = fake_context.processes[1]
        second_child_id = second_process.args[0]
        messages.put(ChildMessage(second_child_id, "running"))
        _wait_for(lambda: pool.ready_count == 2)

        messages.put(ChildMessage(first_child_id, "exiting"))
        _wait_for(first_release.is_set)

        _wait_for(lambda: len(fake_context.processes) == 3)
    finally:
        pool.shutdown()


def test_spawn_children_defers_draining_child_at_min_concurrency() -> None:
    fake_context = _FakeContext()
    pool = _make_fake_context_pool(fake_context, concurrency=2, min_concurrency=1)

    pool.start_spawn_children_thread()
    try:
        _wait_for(lambda: len(fake_context.processes) == 2)
        messages = fake_context.queues[-1]
        first_process = fake_context.processes[0]
        first_child_id = first_process.args[0]
        first_release = first_process.args[-1]

        second_process = fake_context.processes[1]
        second_child_id = second_process.args[0]

        messages.put(ChildMessage(first_child_id, "running"))
        _wait_for(lambda: pool.ready_count == 1)

        messages.put(ChildMessage(first_child_id, "exiting"))
        time.sleep(0.25)
        assert not first_release.is_set()

        messages.put(ChildMessage(second_child_id, "running"))
        _wait_for(first_release.is_set)
    finally:
        pool.shutdown()


def test_spawn_children_replaces_pending_child_that_dies_before_ready() -> None:
    fake_context = _FakeContext()
    pool = _make_fake_context_pool(fake_context, concurrency=1)

    pool.start_spawn_children_thread()
    try:
        _wait_for(lambda: len(fake_context.processes) == 1)
        first_process = fake_context.processes[0]
        first_process.alive = False
        first_process.exitcode = 1

        _wait_for(lambda: len(fake_context.processes) == 2)

        assert first_process.join_calls
        assert fake_context.processes[1].started
    finally:
        pool.shutdown()


def test_shutdown_terminates_all_tracked_children() -> None:
    fake_context = _FakeContext()
    pool = _make_fake_context_pool(fake_context, concurrency=2)

    pool.start_spawn_children_thread()
    _wait_for(lambda: len(fake_context.processes) == 2)

    pool.shutdown()

    assert all(process.terminated for process in fake_context.processes)
    assert all(process.join_calls for process in fake_context.processes)


class TestWorkerServicer(TestCase):
    def test_push_task_success(self) -> None:
        taskworker = PushTaskWorker(
            app_module="examples.app:app",
            broker_service="127.0.0.1:50051",
            max_child_task_count=100,
            process_type="fork",
        )
        with mock.patch.object(
            taskworker.worker_pool, "push_task", return_value=True
        ) as mock_push_task:
            request = PushTaskRequest(
                task=SIMPLE_TASK.activation,
                callback_url="broker-host:50051",
            )
            mock_context = mock.MagicMock()
            servicer = WorkerServicer(taskworker.worker_pool)

            response = servicer.PushTask(request, mock_context)

        self.assertIsInstance(response, PushTaskResponse)
        mock_context.abort.assert_not_called()
        mock_push_task.assert_called_once_with(mock.ANY, timeout=5)
        (inflight,) = mock_push_task.call_args[0]
        self.assertEqual(inflight.activation.id, SIMPLE_TASK.activation.id)
        self.assertEqual(inflight.host, "broker-host:50051")

    def test_push_task_worker_busy(self) -> None:
        taskworker = PushTaskWorker(
            app_module="examples.app:app",
            broker_service="127.0.0.1:50051",
            max_child_task_count=100,
            process_type="fork",
            child_tasks_queue_maxsize=1,
        )
        with mock.patch.object(taskworker.worker_pool, "push_task", return_value=False):
            request = PushTaskRequest(
                task=SIMPLE_TASK.activation,
                callback_url="broker-host:50051",
            )
            mock_context = mock.MagicMock()
            servicer = WorkerServicer(taskworker.worker_pool)

            servicer.PushTask(request, mock_context)

            mock_context.abort.assert_called_once_with(
                grpc.StatusCode.RESOURCE_EXHAUSTED, "worker busy"
            )


@mock.patch("taskbroker_client.worker.workerchild.capture_checkin")
def test_child_process_complete(mock_capture_checkin: mock.MagicMock) -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(SIMPLE_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == SIMPLE_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE
    assert mock_capture_checkin.call_count == 0


def test_child_process_canary_task(capsys: pytest.CaptureFixture[str]) -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(CANARY_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    result = processed.get()
    assert result.task_id == CANARY_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE
    assert capsys.readouterr().out == "Running canary task...\nDone running canary task!\n"


def test_child_process_emits_running_message() -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()
    ctx = get_context("fork")
    child_id = uuid4()
    messages = ctx.Queue()
    parent_release = ctx.Event()
    parent_release.set()

    todo.put(SIMPLE_TASK)
    _child_process(
        child_id,
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
        messages=messages,
        parent_release=parent_release,
    )

    # The child signals readiness once warmup is done, before consuming
    message = messages.get(timeout=1)
    assert message == ChildMessage(child_id, "running")


@mock.patch("taskbroker_client.worker.workerchild.capture_checkin")
def test_child_process_emits_exiting_once_and_continues_until_release(
    mock_capture_checkin: mock.MagicMock,
) -> None:
    shutdown = Event()
    ctx = get_context("fork")
    child_id = uuid4()
    todo = ctx.Queue()
    processed = ctx.Queue()
    messages = ctx.Queue()
    parent_release = ctx.Event()

    todo.put(SIMPLE_TASK)
    process = ctx.Process(
        target=_child_process,
        args=(
            child_id,
            "examples.app:app",
            todo,
            processed,
            shutdown,
            1,
            "test",
            "fork",
            False,
            0.1,
            messages,
            parent_release,
        ),
    )
    process.start()
    try:
        running_message = messages.get(timeout=5)
        busy_message = messages.get(timeout=5)
        idle_message = messages.get(timeout=5)
        exiting_message = messages.get(timeout=5)

        assert running_message == ChildMessage(child_id, "running")
        assert busy_message == ChildMessage(child_id, "busy")
        assert idle_message == ChildMessage(child_id, "idle")
        assert exiting_message == ChildMessage(child_id, "exiting")
        assert processed.get(timeout=5).task_id == SIMPLE_TASK.activation.id

        todo.put(SIMPLE_TASK)
        assert processed.get(timeout=5).task_id == SIMPLE_TASK.activation.id
        assert messages.get(timeout=5) == ChildMessage(child_id, "busy")
        assert messages.get(timeout=5) == ChildMessage(child_id, "idle")

        time.sleep(0.2)
        assert process.is_alive()
        assert messages.empty()

        parent_release.set()
        process.join(timeout=5)
        assert not process.is_alive()
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)

    assert mock_capture_checkin.call_count == 0


def test_child_process_emits_busy_and_idle_messages() -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()
    ctx = get_context("fork")
    child_id = uuid4()
    messages = ctx.Queue()
    parent_release = ctx.Event()
    parent_release.set()

    todo.put(SIMPLE_TASK)
    _child_process(
        child_id,
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
        messages=messages,
        parent_release=parent_release,
    )

    assert messages.get(timeout=1) == ChildMessage(child_id, "running")
    assert messages.get(timeout=1) == ChildMessage(child_id, "busy")
    assert messages.get(timeout=1) == ChildMessage(child_id, "idle")
    assert processed.get(timeout=1).task_id == SIMPLE_TASK.activation.id


def test_child_process_remove_start_time_kwargs() -> None:
    activation = InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id="6789",
            taskname="examples.will_retry",
            namespace="examples",
            parameters_bytes=msgpack.packb(
                {"args": ["stuff"], "kwargs": {"__start_time": 123}}, use_bin_type=True
            ),
            processing_deadline_duration=100000,
        ),
    )
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(activation)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == activation.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


def test_child_process_retry_task() -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(RETRY_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_RETRY


@mock.patch("taskbroker_client.worker.workerchild.logger")
@mock.patch("taskbroker_client.worker.workerchild.sentry_sdk.capture_exception")
def test_child_process_retry_task_max_attempts(
    mock_capture: mock.Mock, mock_logger: mock.Mock
) -> None:
    # Create an activation that is on its final attempt and
    # will raise an error again.
    activation = InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id="6789",
            taskname="examples.will_retry",
            namespace="examples",
            parameters_bytes=msgpack.packb({"args": ["raise"], "kwargs": {}}, use_bin_type=True),
            processing_deadline_duration=100000,
            retry_state=RetryState(
                attempts=2,
                max_attempts=3,
            ),
        ),
    )
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(activation)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == activation.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE

    assert mock_capture.call_count == 1
    capture_call = mock_capture.call_args[0]
    # Error type and chained error should be captured.
    assert isinstance(capture_call[0], NoRetriesRemainingError)
    assert isinstance(capture_call[0].__cause__, RuntimeError)

    # Retry-exhausted emits a structured worker log, but not via
    # logger.exception; the explicit NoRetriesRemainingError capture above
    # remains the only Sentry error event from this branch.
    mock_logger.exception.assert_not_called()
    mock_logger.warning.assert_called_once()
    args, kwargs = mock_logger.warning.call_args
    assert args[0] == "taskworker.task.retry_exhausted"
    extra = kwargs["extra"]
    assert extra["exception_type"] == "RuntimeError"
    assert extra["taskname"] == "examples.will_retry"
    assert extra["retry_attempts"] == 2
    assert extra["retry_max_attempts"] == 3


def test_child_process_failure_task() -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(FAIL_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == FAIL_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE


def test_child_process_shutdown() -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()
    shutdown.set()

    todo.put(SIMPLE_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    # When shutdown has been set, the child should not process more tasks.
    assert todo.qsize() == 1
    assert processed.qsize() == 0


def test_child_process_unknown_task() -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(UNDEFINED_TASK)
    todo.put(SIMPLE_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    result = processed.get()
    assert result.task_id == UNDEFINED_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE

    result = processed.get()
    assert result.task_id == SIMPLE_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


def test_child_process_at_most_once() -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(AT_MOST_ONCE_TASK)
    todo.put(AT_MOST_ONCE_TASK)
    todo.put(SIMPLE_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=2,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get(block=False)
    assert result.task_id == AT_MOST_ONCE_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE

    result = processed.get(block=False)
    assert result.task_id == SIMPLE_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


@mock.patch("taskbroker_client.worker.workerchild.capture_checkin")
def test_child_process_record_checkin(mock_capture_checkin: mock.Mock) -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(SCHEDULED_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == SIMPLE_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE

    assert mock_capture_checkin.call_count == 1
    mock_capture_checkin.assert_called_with(
        monitor_slug="simple-task",
        check_in_id="abc123",
        duration=mock.ANY,
        status=MonitorStatus.OK,
    )


def test_child_process_pass_headers() -> None:
    """Task with pass_headers=True receives headers from the activation."""
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(TASK_WITH_HEADERS)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == TASK_WITH_HEADERS.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE

    redis = StrictRedis(host="localhost", port=6379, decode_responses=True)
    assert redis.get("task-headers-value") == "test_value"
    assert redis.get("task-headers-custom") == "custom_value"
    redis.delete("task-headers-value", "task-headers-count", "task-headers-custom")


@mock.patch("taskbroker_client.worker.workerchild.logger")
def test_child_process_terminate_task(mock_logger: mock.Mock) -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    sleepy = InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id="111",
            taskname="examples.timed",
            namespace="examples",
            parameters_bytes=msgpack.packb({"args": [3], "kwargs": {}}, use_bin_type=True),
            processing_deadline_duration=1,
        ),
    )

    todo.put(sleepy)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get(block=False)
    assert result.task_id == sleepy.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE
    assert mock_logger.exception.call_count == 1
    args, kwargs = mock_logger.exception.call_args
    assert args[0] == "taskworker.task.failed"
    extra = kwargs["extra"]
    assert extra["exception_type"] == "ProcessingDeadlineExceeded"
    assert extra["taskname"] == "examples.timed"
    assert extra["namespace"] == "examples"
    assert extra["task_id"] == "111"
    assert extra["processing_pool"] == "test"
    assert "execution deadline" in extra["exception_message"]


@mock.patch("taskbroker_client.worker.workerchild.capture_checkin")
def test_child_process_decompression(mock_capture_checkin: mock.MagicMock) -> None:

    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(COMPRESSED_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == COMPRESSED_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE
    assert mock_capture_checkin.call_count == 0


def test_child_process_context_hooks() -> None:
    """Context hooks' on_execute is called with activation headers during task execution."""
    executed_headers: list[dict[str, str]] = []

    class RecordingHook:
        def on_dispatch(self, headers: MutableMapping[str, Any]) -> None:
            pass

        def on_execute(self, headers: dict[str, str]) -> contextlib.AbstractContextManager[None]:
            executed_headers.append(dict(headers))
            return contextlib.nullcontext()

    from examples.app import app

    hook = RecordingHook()
    app.context_hooks.append(hook)

    try:
        activation_with_headers = InflightTaskActivation(
            host="localhost:50051",
            receive_timestamp=0,
            activation=TaskActivation(
                id="hook-test",
                taskname="examples.simple_task",
                namespace="examples",
                parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
                headers={"x-viewer-org": "42", "x-viewer-user": "7"},
                processing_deadline_duration=5,
            ),
        )

        todo: queue.Queue[InflightTaskActivation] = queue.Queue()
        processed: queue.Queue[ProcessingResult] = queue.Queue()
        shutdown = Event()

        todo.put(activation_with_headers)
        child_process(
            "examples.app:app",
            todo,
            processed,
            shutdown,
            max_task_count=1,
            processing_pool_name="test",
            process_type="fork",
            skip_awaiting_futures=False,
            future_checking_frequency=0.1,
        )

        result = processed.get()
        assert result.status == TASK_ACTIVATION_STATUS_COMPLETE
        assert len(executed_headers) == 1
        assert executed_headers[0]["x-viewer-org"] == "42"
        assert executed_headers[0]["x-viewer-user"] == "7"
    finally:
        app.context_hooks.remove(hook)


@mock.patch("taskbroker_client.worker.workerchild.logger")
def test_child_process_silenced_timeout(mock_logger: mock.Mock) -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(RETRY_TASK_WITH_SILENCED_TIMEOUT)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK_WITH_SILENCED_TIMEOUT.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE
    failed_calls = [
        c
        for c in mock_logger.exception.call_args_list
        if c.args and c.args[0] == "taskworker.task.failed"
    ]
    assert failed_calls == []


@mock.patch("taskbroker_client.worker.workerchild.sentry_sdk.capture_exception")
def test_child_process_silenced_exception_with_retries(mock_capture: mock.Mock) -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(RETRY_TASK_WITH_SILENCED_UNHANDLED_EXCEPTION)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK_WITH_SILENCED_UNHANDLED_EXCEPTION.activation.id

    # No reporting, but the task still raised an unhandled exception
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE
    assert mock_capture.call_count == 0


@mock.patch("taskbroker_client.worker.workerchild.sentry_sdk.capture_exception")
def test_child_process_expected_ignored_exception_max_attempts(mock_capture: mock.Mock) -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    # Task has more retries left, but is set to ignore the raised error type
    todo.put(RETRY_TASK_WITH_SILENCED_IGNORED_EXCEPTION)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    # No reporting, but exception type is retriable
    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK_WITH_SILENCED_IGNORED_EXCEPTION.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_RETRY
    assert mock_capture.call_count == 0


@mock.patch("taskbroker_client.worker.workerchild.logger")
@mock.patch("taskbroker_client.worker.workerchild.sentry_sdk.capture_exception")
def test_child_process_silenced_exception_max_attempts(
    mock_capture: mock.Mock, mock_logger: mock.Mock
) -> None:
    """Silenced exceptions do not raise on retry exhaustion."""
    activation = InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id="silenced-max-attempts",
            taskname="examples.will_fail_with_silenced_ignored_exception",
            namespace="examples",
            parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
            processing_deadline_duration=2,
            retry_state=RetryState(
                # No retries left
                attempts=1,
                max_attempts=2,
                on_attempts_exceeded=ON_ATTEMPTS_EXCEEDED_DISCARD,
            ),
        ),
    )
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(activation)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == activation.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE

    # Silenced error: no Sentry event even though retries are exhausted.
    assert mock_capture.call_count == 0

    # The structured retry-exhausted log still fires (without logger.exception).
    mock_logger.exception.assert_not_called()
    mock_logger.warning.assert_called_once()
    args, kwargs = mock_logger.warning.call_args
    assert args[0] == "taskworker.task.retry_exhausted"
    assert kwargs["extra"]["exception_type"] == "RuntimeError"


@mock.patch("taskbroker_client.worker.workerchild.logger")
def test_child_process_retry_on_deadline_exceeded(mock_logger: mock.Mock) -> None:
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    # Task will timeout, but should retry, because ProcessingDeadlineExceeded is
    # in the Retry.on list
    todo.put(RETRY_TASK_ON_DEADLINE_EXCEEDED)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK_ON_DEADLINE_EXCEEDED.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_RETRY
    # The timeout was reported (report_timeout_errors=True) even though
    # the task will retry. taskworker.task.failed fires once per attempt.
    assert mock_logger.exception.call_count == 1
    args, kwargs = mock_logger.exception.call_args
    assert args[0] == "taskworker.task.failed"
    assert kwargs["extra"]["exception_type"] == "ProcessingDeadlineExceeded"


@mock.patch("taskbroker_client.worker.workerchild.logger")
def test_child_process_general_exception_logs_task_failed(mock_logger: mock.Mock) -> None:
    """A non-retriable Exception emits taskworker.task.failed with all fields."""
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    # examples.fail_task has no retry policy → raises ValueError once,
    # task fails terminally on first attempt.
    todo.put(FAIL_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    result = processed.get()
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE
    assert mock_logger.exception.call_count == 1
    args, kwargs = mock_logger.exception.call_args
    assert args[0] == "taskworker.task.failed"
    extra = kwargs["extra"]
    assert extra["task_id"] == "333"
    assert extra["taskname"] == "examples.fail_task"
    assert extra["namespace"] == "examples"
    assert extra["processing_pool"] == "test"
    assert extra["exception_type"] == "ValueError"
    assert "exception_message" in extra


@mock.patch("taskbroker_client.worker.workerchild.logger")
def test_child_process_silenced_exception_does_not_log_task_failed(
    mock_logger: mock.Mock,
) -> None:
    """When err is in silenced_exceptions, taskworker.task.failed is NOT logged.
    Preserves the silencing semantics added in #608."""
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(RETRY_TASK_WITH_SILENCED_UNHANDLED_EXCEPTION)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    result = processed.get()
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE
    # Other log calls (e.g., taskworker.task.retry) may have fired, but
    # taskworker.task.failed must not have.
    failed_calls = [
        c
        for c in mock_logger.exception.call_args_list
        if c.args and c.args[0] == "taskworker.task.failed"
    ]
    assert failed_calls == []


# Tests for producer future tracking, storage, and drain-on-shutdown behavior
# in child_process. These tests patch FutureTrackingProducer.collect_futures so we can inject
# controllable futures without needing a real Kafka broker.


@pytest.fixture
def clear_pending_futures() -> Iterator[None]:
    _arroyo_pending_futures.clear()
    yield
    _arroyo_pending_futures.clear()


@pytest.fixture
def restore_signal_handlers() -> Iterator[None]:
    """`child_process` installs SIGTERM/SIGINT handlers in the current process."""
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    prev_sigint = signal.getsignal(signal.SIGINT)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, prev_sigterm)
        signal.signal(signal.SIGINT, prev_sigint)


def _make_broker_value() -> BrokerValue[KafkaPayload]:
    return BrokerValue(
        KafkaPayload(None, b"", []),
        Partition(Topic("test"), 0),
        0,
        datetime(2024, 1, 1),
    )


def _producing_task(task_id: str = "task-with-futures") -> InflightTaskActivation:
    return InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id=task_id,
            taskname="examples.simple_task",
            namespace="examples",
            parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
            processing_deadline_duration=2,
        ),
    )


def test_child_process_tracks_producer_futures(
    clear_pending_futures: None,
    restore_signal_handlers: None,
) -> None:
    task = _producing_task()
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    done_future: Future[BrokerValue[KafkaPayload]] = Future()
    done_future.set_result(_make_broker_value())

    todo.put(task)
    with mock.patch.object(
        FutureTrackingProducer, "collect_futures", return_value={"test.producer": {done_future}}
    ) as collect_mock:
        child_process(
            "examples.app:app",
            todo,
            processed,
            shutdown,
            max_task_count=1,
            processing_pool_name="test",
            process_type="fork",
            skip_awaiting_futures=False,
            future_checking_frequency=0.1,
        )

    # collect_futures is called once per executed task
    assert collect_mock.call_count == 1

    result = processed.get(timeout=5)
    assert result.task_id == task.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


def test_child_process_ignores_empty_producer_future_sets(
    clear_pending_futures: None,
    restore_signal_handlers: None,
) -> None:
    task = _producing_task()
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(task)
    with (
        mock.patch.object(
            FutureTrackingProducer, "collect_futures", return_value={"test.producer": set()}
        ),
        mock.patch(
            "taskbroker_client.worker.workerchild.ActivationWithPendingFutures"
        ) as pending_task,
    ):
        child_process(
            "examples.app:app",
            todo,
            processed,
            shutdown,
            max_task_count=1,
            processing_pool_name="test",
            process_type="fork",
            skip_awaiting_futures=False,
            future_checking_frequency=0.1,
        )

    pending_task.assert_not_called()
    result = processed.get(timeout=5)
    assert result.task_id == task.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


def test_child_process_holds_result_until_futures_done(
    clear_pending_futures: None,
    restore_signal_handlers: None,
) -> None:
    task = _producing_task()
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    pending_future: Future[BrokerValue[KafkaPayload]] = Future()
    todo.put(task)

    # `child_process` calls `signal.signal`, which must run on the main thread.
    # Use a helper thread to observe the queue while the future is still
    # pending, then resolve the future so the drain can complete.
    observed_empty_while_pending = threading.Event()

    def observe_and_resolve() -> None:
        # Wait for child_process to process the task and enter the drain loop.
        time.sleep(0.5)
        if processed.qsize() == 0:
            observed_empty_while_pending.set()
        pending_future.set_result(_make_broker_value())

    observer = threading.Thread(target=observe_and_resolve, name="future-observer")
    observer.start()
    try:
        with mock.patch.object(
            FutureTrackingProducer,
            "collect_futures",
            return_value={"test.producer": {pending_future}},
        ):
            child_process(
                "examples.app:app",
                todo,
                processed,
                shutdown,
                max_task_count=1,
                processing_pool_name="test",
                process_type="fork",
                skip_awaiting_futures=False,
                future_checking_frequency=0.1,
            )
    finally:
        observer.join(timeout=5)
        shutdown.set()

    assert (
        observed_empty_while_pending.is_set()
    ), "result was pushed before the producer future was resolved"
    result = processed.get(timeout=5)
    assert result.task_id == task.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


def test_child_process_skip_awaiting_futures_places_result_immediately(
    clear_pending_futures: None,
    restore_signal_handlers: None,
) -> None:
    task = _producing_task()
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    pending_future: Future[BrokerValue[KafkaPayload]] = Future()
    todo.put(task)

    # With skip_awaiting_futures=True the ProcessingResult is placed as soon as
    # the task function finishes executing, without waiting for the producer
    # future to resolve. Observe the queue while the future is still pending to
    # prove the result is available immediately, then resolve the future so the
    # drain loop can complete.
    observed_result_while_pending = threading.Event()

    def observe_and_resolve() -> None:
        start = time.time()
        while time.time() - start < 2:
            if processed.qsize() > 0:
                observed_result_while_pending.set()
                break
            time.sleep(0.01)
        pending_future.set_result(_make_broker_value())

    observer = threading.Thread(target=observe_and_resolve, name="future-observer")
    observer.start()
    try:
        with mock.patch.object(
            FutureTrackingProducer,
            "collect_futures",
            return_value={"test.producer": {pending_future}},
        ):
            child_process(
                "examples.app:app",
                todo,
                processed,
                shutdown,
                max_task_count=1,
                processing_pool_name="test",
                process_type="fork",
                skip_awaiting_futures=True,
                future_checking_frequency=0.1,
            )
    finally:
        observer.join(timeout=5)
        shutdown.set()

    assert (
        observed_result_while_pending.is_set()
    ), "result was not placed immediately after the task function executed"
    result = processed.get(timeout=5)
    assert result.task_id == task.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE
    # Awaiting the futures afterwards must not enqueue a second ProcessingResult
    # (the immediate placement is the only result).
    assert processed.empty()


def test_child_process_drains_pending_futures_on_sigterm(
    clear_pending_futures: None,
    restore_signal_handlers: None,
) -> None:
    task = _producing_task()
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    pending_future: Future[BrokerValue[KafkaPayload]] = Future()
    todo.put(task)

    def deliver_sigterm() -> None:
        # Wait for child_process to install its SIGTERM handler and start the
        # worker loop, otherwise the default handler would terminate the test
        # process.
        time.sleep(0.5)
        pending_future.set_result(_make_broker_value())
        os.kill(os.getpid(), signal.SIGTERM)

    sigterm_thread = threading.Thread(target=deliver_sigterm, name="sigterm-sender")
    sigterm_thread.start()
    try:
        with mock.patch.object(
            FutureTrackingProducer,
            "collect_futures",
            return_value={"test.producer": {pending_future}},
        ):
            child_process(
                "examples.app:app",
                todo,
                processed,
                shutdown,
                max_task_count=None,
                processing_pool_name="test",
                process_type="fork",
                skip_awaiting_futures=False,
                future_checking_frequency=0.1,
            )
    finally:
        sigterm_thread.join(timeout=5)
        shutdown.set()

    result = processed.get(timeout=5)
    assert result.task_id == task.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


def test_child_process_retries_on_failed_future(
    clear_pending_futures: None,
    restore_signal_handlers: None,
) -> None:
    retriable_task = InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id="failed-future-retry",
            taskname="examples.will_retry",
            namespace="examples",
            parameters_bytes=msgpack.packb({"args": ["noop"], "kwargs": {}}, use_bin_type=True),
            processing_deadline_duration=2,
            retry_state=RetryState(
                attempts=0,
                max_attempts=3,
                on_attempts_exceeded=ON_ATTEMPTS_EXCEEDED_DISCARD,
            ),
        ),
    )
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    failed_future: Future[BrokerValue[KafkaPayload]] = Future()
    failed_future.set_exception(RuntimeError("kafka produce failed"))

    todo.put(retriable_task)
    with mock.patch.object(
        FutureTrackingProducer, "collect_futures", return_value={"test.producer": {failed_future}}
    ):
        child_process(
            "examples.app:app",
            todo,
            processed,
            shutdown,
            max_task_count=1,
            processing_pool_name="test",
            process_type="fork",
            skip_awaiting_futures=False,
            future_checking_frequency=0.1,
        )

    result = processed.get(timeout=5)
    assert result.task_id == retriable_task.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_RETRY


def test_child_process_clears_pending_futures_when_task_fails(
    clear_pending_futures: None,
    restore_signal_handlers: None,
) -> None:
    leftover_future: Future[BrokerValue[KafkaPayload]] = Future()
    leftover_future.set_result(_make_broker_value())
    _arroyo_pending_futures["test.producer"].append(leftover_future)
    assert len(_arroyo_pending_futures) == 1

    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(FAIL_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
        skip_awaiting_futures=False,
        future_checking_frequency=0.1,
    )

    result = processed.get(timeout=5)
    assert result.task_id == FAIL_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE

    # The orphaned future is dropped (the activation will be retried at the
    # broker level if applicable) but the global registry is cleared so it
    # cannot bleed into the next task this child processes.
    assert len(_arroyo_pending_futures) == 0


def test_child_process_uses_configured_future_checking_frequency(
    clear_pending_futures: None, restore_signal_handlers: None
) -> None:
    """The idle future-checking loop polls on the configured interval."""
    # A task that runs long enough for the idle future-checking loop to poll a
    # few times before max_task_count triggers shutdown.
    slow_task = InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id="freq-task",
            taskname="examples.timed",
            namespace="examples",
            parameters_bytes=msgpack.packb({"args": [0.5], "kwargs": {}}, use_bin_type=True),
            processing_deadline_duration=5,
        ),
    )
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()
    todo.put(slow_task)

    configured_frequency = 0.05
    idle_sleeps: list[float] = []
    real_sleep = time.sleep

    def recording_sleep(seconds: float) -> None:
        idle_sleeps.append(seconds)
        real_sleep(seconds)

    # time.sleep is only used by the idle branch of check_task_future_completion
    # inside workerchild, so every recorded call comes from that loop. The task's
    # own sleep uses a separate `from time import sleep` import in examples.tasks.
    with mock.patch("taskbroker_client.worker.workerchild.time.sleep", side_effect=recording_sleep):
        child_process(
            "examples.app:app",
            todo,
            processed,
            shutdown,
            max_task_count=1,
            processing_pool_name="test",
            process_type="fork",
            skip_awaiting_futures=False,
            future_checking_frequency=configured_frequency,
        )

    result = processed.get(timeout=5)
    assert result.task_id == slow_task.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE

    # The idle future-checking loop ran and polled using the configured
    # frequency for every iteration.
    assert idle_sleeps, "future-checking thread never slept while idle"
    assert all(seconds == configured_frequency for seconds in idle_sleeps)
