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
    TaskWorker,
    TaskWorkerProcessingPool,
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


def test_start_does_not_serve_when_shutdown_during_warmup() -> None:
    from grpc_health.v1 import health_pb2

    taskworker = _make_push_worker(concurrency=2, warmup_timeout=5)
    # Children never warm, and shutdown is requested before start() runs.
    taskworker._grpc_sync_event.set()

    fake_health = mock.MagicMock()
    fake_server = mock.MagicMock()

    with (
        mock.patch.object(taskworker.worker_pool, "start_metrics_thread"),
        mock.patch.object(taskworker.worker_pool, "start_result_thread"),
        mock.patch.object(taskworker.worker_pool, "start_spawn_children_thread"),
        mock.patch.object(taskworker.worker_pool, "shutdown"),
        mock.patch("taskbroker_client.worker.worker.grpc.server", return_value=fake_server),
        mock.patch(
            "taskbroker_client.worker.worker.health.HealthServicer", return_value=fake_health
        ),
        mock.patch("taskbroker_client.worker.worker.health_pb2_grpc.add_HealthServicer_to_server"),
        mock.patch(
            "taskbroker_client.worker.worker.taskbroker_pb2_grpc"
            ".add_WorkerServiceServicer_to_server"
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
    # We never reached server.wait_for_termination() (returned before it).
    fake_server.wait_for_termination.assert_not_called()


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
    with mock.patch("taskbroker_client.canary.logger") as mock_logger:
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
    mock_logger.info.assert_called_once_with("Running canary task...")
    assert capsys.readouterr().out == "Done running canary task!\n"


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
