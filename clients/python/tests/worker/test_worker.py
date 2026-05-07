import base64
import contextlib
import queue
import time
from collections.abc import MutableMapping
from multiprocessing import Event, get_context
from typing import Any
from unittest import TestCase, mock

import grpc
import msgpack
import orjson
import zstandard as zstd
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

from taskbroker_client.constants import CompressionType
from taskbroker_client.retry import NoRetriesRemainingError
from taskbroker_client.state import current_task
from taskbroker_client.types import InflightTaskActivation, ProcessingResult
from taskbroker_client.worker.worker import (
    PushTaskWorker,
    TaskWorker,
    TaskWorkerProcessingPool,
    WorkerServicer,
)
from taskbroker_client.worker.workerchild import ProcessingDeadlineExceeded, child_process

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

# Legacy fixture using the old JSON parameters field for backward compat testing
LEGACY_JSON_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="legacy_json_123",
        taskname="examples.simple_task",
        namespace="examples",
        parameters='{"args": ["legacy_arg"], "kwargs": {}}',
        processing_deadline_duration=2,
    ),
)

# Legacy compressed fixture using base64+zstd in the old parameters field
LEGACY_COMPRESSED_TASK = InflightTaskActivation(
    host="localhost:50051",
    receive_timestamp=0,
    activation=TaskActivation(
        id="legacy_compressed_123",
        taskname="examples.simple_task",
        namespace="examples",
        parameters=base64.b64encode(
            zstd.compress(
                orjson.dumps(
                    {
                        "args": ["test_arg1", "test_arg2"],
                        "kwargs": {"test_key": "test_value", "number": 42},
                    }
                )
            )
        ).decode("utf8"),
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
            assert mock_client.update_task.call_count == 1
            assert mock_client.update_task.call_args.args[0].host == "localhost:50051"
            assert mock_client.update_task.call_args.args[0].task_id == SIMPLE_TASK.activation.id
            assert (
                mock_client.update_task.call_args.args[0].status == TASK_ACTIVATION_STATUS_COMPLETE
            )
            assert mock_client.update_task.call_args.args[1] is None

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
            assert mock_client.update_task.call_count == 2
            assert mock_client.update_task.call_args.args[0].host == "localhost:50051"
            assert mock_client.update_task.call_args.args[0].task_id == SIMPLE_TASK.activation.id
            assert (
                mock_client.update_task.call_args.args[0].status == TASK_ACTIVATION_STATUS_COMPLETE
            )
            assert mock_client.update_task.call_args.args[1] is None

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
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == SIMPLE_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE
    assert mock_capture_checkin.call_count == 0


def test_child_process_remove_start_time_kwargs() -> None:
    activation = InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id="6789",
            taskname="examples.will_retry",
            namespace="examples",
            parameters='{"args": ["stuff"], "kwargs": {"__start_time": 123}}',
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
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_RETRY


@mock.patch("taskbroker_client.worker.workerchild.sentry_sdk.capture_exception")
def test_child_process_retry_task_max_attempts(mock_capture: mock.Mock) -> None:
    # Create an activation that is on its final attempt and
    # will raise an error again.
    activation = InflightTaskActivation(
        host="localhost:50051",
        receive_timestamp=0,
        activation=TaskActivation(
            id="6789",
            taskname="examples.will_retry",
            namespace="examples",
            parameters='{"args": ["raise"], "kwargs": {}}',
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
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == TASK_WITH_HEADERS.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE

    redis = StrictRedis(host="localhost", port=6379, decode_responses=True)
    assert redis.get("task-headers-value") == "test_value"
    assert redis.get("task-headers-custom") == "custom_value"
    redis.delete("task-headers-value", "task-headers-count", "task-headers-custom")


@mock.patch("taskbroker_client.worker.workerchild.sentry_sdk.capture_exception")
def test_child_process_terminate_task(mock_capture: mock.Mock) -> None:
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
            parameters='{"args": [3], "kwargs": {}}',
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
    )

    assert todo.empty()
    result = processed.get(block=False)
    assert result.task_id == sleepy.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE
    assert mock_capture.call_count == 1
    assert type(mock_capture.call_args.args[0]) is ProcessingDeadlineExceeded


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
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == COMPRESSED_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE
    assert mock_capture_checkin.call_count == 0


@mock.patch("sentry_sdk.crons.api.capture_checkin")
def test_child_process_legacy_json_parameters(mock_capture_checkin: mock.MagicMock) -> None:
    """Test backward compat: worker can handle legacy JSON parameters field."""
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(LEGACY_JSON_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == LEGACY_JSON_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


@mock.patch("sentry_sdk.crons.api.capture_checkin")
def test_child_process_legacy_compressed_parameters(mock_capture_checkin: mock.MagicMock) -> None:
    """Test backward compat: worker can handle legacy base64+zstd compressed JSON parameters."""
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    shutdown = Event()

    todo.put(LEGACY_COMPRESSED_TASK)
    child_process(
        "examples.app:app",
        todo,
        processed,
        shutdown,
        max_task_count=1,
        processing_pool_name="test",
        process_type="fork",
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == LEGACY_COMPRESSED_TASK.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_COMPLETE


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
                parameters='{"args": [], "kwargs": {}}',
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
        )

        result = processed.get()
        assert result.status == TASK_ACTIVATION_STATUS_COMPLETE
        assert len(executed_headers) == 1
        assert executed_headers[0]["x-viewer-org"] == "42"
        assert executed_headers[0]["x-viewer-user"] == "7"
    finally:
        app.context_hooks.remove(hook)


@mock.patch("taskbroker_client.worker.workerchild.sentry_sdk.capture_exception")
def test_child_process_silenced_timeout(mock_capture: mock.Mock) -> None:
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
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK_WITH_SILENCED_TIMEOUT.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_FAILURE
    assert mock_capture.call_count == 0


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
    )

    # No reporting, but exception type is retriable
    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK_WITH_SILENCED_IGNORED_EXCEPTION.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_RETRY
    assert mock_capture.call_count == 0


@mock.patch("taskbroker_client.worker.workerchild.sentry_sdk.capture_exception")
def test_child_process_retry_on_deadline_exceeded(mock_capture: mock.Mock) -> None:
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
    )

    assert todo.empty()
    result = processed.get()
    assert result.task_id == RETRY_TASK_ON_DEADLINE_EXCEEDED.activation.id
    assert result.status == TASK_ACTIVATION_STATUS_RETRY
    assert mock_capture.call_count == 1
    assert type(mock_capture.call_args.args[0]) is ProcessingDeadlineExceeded
