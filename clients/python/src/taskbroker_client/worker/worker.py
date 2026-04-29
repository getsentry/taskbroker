from __future__ import annotations

import logging
import multiprocessing
import queue
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, List

import grpc
from sentry_protos.taskbroker.v1 import taskbroker_pb2_grpc
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    FetchNextTask,
    PushTaskRequest,
    PushTaskResponse,
)

from taskbroker_client.app import import_app
from taskbroker_client.constants import (
    DEFAULT_REBALANCE_AFTER,
    DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
    DEFAULT_WORKER_QUEUE_SIZE,
    MAX_BACKOFF_SECONDS_WHEN_HOST_UNAVAILABLE,
)
from taskbroker_client.types import InflightTaskActivation, ProcessingResult
from taskbroker_client.worker.client import (
    HealthCheckSettings,
    HostTemporarilyUnavailable,
    PushTaskbrokerClient,
    RequestSignatureServerInterceptor,
    TaskbrokerClient,
    parse_rpc_secret_list,
)
from taskbroker_client.worker.workerchild import child_process

if TYPE_CHECKING:
    ServerInterceptor = grpc.ServerInterceptor[Any, Any]
else:
    ServerInterceptor = grpc.ServerInterceptor


logger = logging.getLogger(__name__)


class WorkerServicer(taskbroker_pb2_grpc.WorkerServiceServicer):
    """
    gRPC servicer that receives task activations pushed from the broker
    """

    def __init__(self, worker: TaskWorkerProcessingPool) -> None:
        self.worker_pool = worker

    def PushTask(
        self,
        request: PushTaskRequest,
        context: grpc.ServicerContext,
    ) -> PushTaskResponse:
        """Handle incoming task activation."""
        start_time = time.monotonic()
        self.worker_pool._metrics.incr(
            "taskworker.worker.push_rpc",
            tags={"result": "attempt", "processing_pool": self.worker_pool._processing_pool_name},
        )

        # Create `InflightTaskActivation` from the pushed task
        inflight = InflightTaskActivation(
            activation=request.task,
            host=request.callback_url,
            receive_timestamp=time.monotonic(),
        )

        # Push the task to the worker queue (wait at most 5 seconds)
        if not self.worker_pool.push_task(inflight, timeout=5):
            self.worker_pool._metrics.incr(
                "taskworker.worker.push_rpc",
                tags={"result": "busy", "processing_pool": self.worker_pool._processing_pool_name},
            )

            self.worker_pool._metrics.distribution(
                "taskworker.worker.push_rpc.duration",
                time.monotonic() - start_time,
                tags={"result": "busy", "processing_pool": self.worker_pool._processing_pool_name},
            )

            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "worker busy")
        else:
            self.worker_pool._metrics.incr(
                "taskworker.worker.push_rpc",
                tags={
                    "result": "accepted",
                    "processing_pool": self.worker_pool._processing_pool_name,
                },
            )

            self.worker_pool._metrics.distribution(
                "taskworker.worker.push_rpc.duration",
                time.monotonic() - start_time,
                tags={
                    "result": "accepted",
                    "processing_pool": self.worker_pool._processing_pool_name,
                },
            )

        return PushTaskResponse()


class RequeueException(Exception):
    pass


class PushTaskWorker:
    _mp_context: ForkContext | SpawnContext | ForkServerContext

    def __init__(
        self,
        app_module: str,
        broker_service: str,
        max_child_task_count: int | None = None,
        namespace: str | None = None,
        concurrency: int = 1,
        child_tasks_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        result_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        rebalance_after: int = DEFAULT_REBALANCE_AFTER,
        processing_pool_name: str | None = None,
        pod_name: str | None = None,
        process_type: str = "spawn",
        health_check_file_path: str | None = None,
        health_check_sec_per_touch: float = DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
        grpc_port: int = 50052,
    ) -> None:
        app = import_app(app_module)

        if process_type == "fork":
            self._mp_context = multiprocessing.get_context("fork")
        elif process_type == "spawn":
            self._mp_context = multiprocessing.get_context("spawn")
        elif process_type == "forkserver":
            self._mp_context = multiprocessing.get_context("forkserver")
        else:
            raise ValueError(f"Invalid process type: {process_type}")

        self.worker_pool = TaskWorkerProcessingPool(
            app_module=app_module,
            mp_context=self._mp_context,
            send_result_fn=self._send_result,
            max_child_task_count=max_child_task_count,
            concurrency=concurrency,
            child_tasks_queue_maxsize=child_tasks_queue_maxsize,
            result_queue_maxsize=result_queue_maxsize,
            processing_pool_name=processing_pool_name,
            pod_name=pod_name,
            process_type=process_type,
        )

        logger.info("Running in PUSH mode")

        self.client = PushTaskbrokerClient(
            service=broker_service,
            application=app.name,
            metrics=app.metrics,
            health_check_settings=(
                None
                if health_check_file_path is None
                else HealthCheckSettings(Path(health_check_file_path), health_check_sec_per_touch)
            ),
            rpc_secret=app.config["rpc_secret"],
            grpc_config=app.config["grpc_config"],
        )
        self._metrics = app.metrics
        self._concurrency = concurrency
        self._grpc_sync_event = self._mp_context.Event()

        self._setstatus_backoff_seconds = 0

        self._processing_pool_name: str = processing_pool_name or "unknown"

        self._grpc_port = grpc_port
        self._grpc_secrets = parse_rpc_secret_list(app.config["rpc_secret"])

    def _send_result(
        self, result: ProcessingResult, is_draining: bool = False
    ) -> InflightTaskActivation | None:
        """
        Send a result to the broker. If the set has failed before, sleep briefly before retrying.
        """
        self._metrics.distribution(
            "taskworker.worker.complete_duration",
            time.monotonic() - result.receive_timestamp,
            tags={"processing_pool": self._processing_pool_name},
        )

        logger.debug(
            "taskworker.workers._send_result",
            extra={
                "task_id": result.task_id,
                "next": False,  # Push mode doesn't support fetching next tasks
                "processing_pool": self._processing_pool_name,
            },
        )
        # Use the shutdown_event as a sleep mechanism
        self._grpc_sync_event.wait(self._setstatus_backoff_seconds)

        try:
            self.client.update_task(result)
            self._setstatus_backoff_seconds = 0
            return None
        except grpc.RpcError as e:
            self._setstatus_backoff_seconds = min(self._setstatus_backoff_seconds + 1, 10)
            logger.warning(
                "taskworker.send_update_task.failed",
                extra={"task_id": result.task_id, "error": e},
            )
            if e.code() != grpc.StatusCode.NOT_FOUND:
                # If the task was not found, we can't update it, so we should just return None
                raise RequeueException(f"Failed to update task: {e}")
        except HostTemporarilyUnavailable as e:
            self._setstatus_backoff_seconds = min(
                self._setstatus_backoff_seconds + 4, MAX_BACKOFF_SECONDS_WHEN_HOST_UNAVAILABLE
            )
            logger.info(
                "taskworker.send_update_task.temporarily_unavailable",
                extra={"task_id": result.task_id, "error": str(e)},
            )
            raise RequeueException(f"Failed to update task: {e}")

        return None

    def start(self) -> int:
        """
        This starts the worker gRPC server.
        """
        self.worker_pool.start_result_thread()
        self.worker_pool.start_spawn_children_thread()

        # Convert signals into KeyboardInterrupt.
        # Running shutdown() within the signal handler can lead to deadlocks

        server: grpc.Server | None = None

        def signal_handler(*args: Any) -> None:
            if server:
                server.stop(grace=5)
            raise KeyboardInterrupt()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            # Start gRPC server
            interceptors: List[ServerInterceptor] = []

            if self._grpc_secrets:
                interceptors = [RequestSignatureServerInterceptor(self._grpc_secrets)]

            server = grpc.server(
                ThreadPoolExecutor(max_workers=self._concurrency),
                interceptors=interceptors,
            )

            taskbroker_pb2_grpc.add_WorkerServiceServicer_to_server(
                WorkerServicer(self.worker_pool), server
            )
            server.add_insecure_port(f"[::]:{self._grpc_port}")
            server.start()
            logger.info("taskworker.grpc_server.started", extra={"port": self._grpc_port})

            try:
                server.wait_for_termination()
            except KeyboardInterrupt:
                # Signals are converted to KeyboardInterrupt, swallow for exit code 0
                pass

        finally:
            if server is not None:
                server.stop(grace=5)

            self.worker_pool.shutdown()

        return 0

    def shutdown(self) -> None:
        """
        Shutdown the worker.
        """
        self._grpc_sync_event.set()
        self.worker_pool.shutdown()


class TaskWorker:
    """
    A TaskWorker fetches tasks from a taskworker RPC host and handles executing task activations.

    Tasks are executed in a forked/spawned/forkserver process so that processing timeouts can be enforced.
    As tasks are completed status changes will be sent back to the RPC host and new tasks
    will be fetched.
    """

    _mp_context: ForkContext | SpawnContext | ForkServerContext

    def __init__(
        self,
        app_module: str,
        broker_hosts: list[str],
        max_child_task_count: int | None = None,
        namespace: str | None = None,
        concurrency: int = 1,
        child_tasks_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        result_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        rebalance_after: int = DEFAULT_REBALANCE_AFTER,
        processing_pool_name: str | None = None,
        process_type: str = "spawn",
        health_check_file_path: str | None = None,
        health_check_sec_per_touch: float = DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
    ) -> None:
        self._namespace = namespace
        app = import_app(app_module)

        if process_type == "fork":
            self._mp_context = multiprocessing.get_context("fork")
        elif process_type == "spawn":
            self._mp_context = multiprocessing.get_context("spawn")
        elif process_type == "forkserver":
            self._mp_context = multiprocessing.get_context("forkserver")
        else:
            raise ValueError(f"Invalid process type: {process_type}")

        self.worker_pool = TaskWorkerProcessingPool(
            app_module=app_module,
            mp_context=self._mp_context,
            send_result_fn=self._send_result,
            max_child_task_count=max_child_task_count,
            concurrency=concurrency,
            child_tasks_queue_maxsize=child_tasks_queue_maxsize,
            result_queue_maxsize=result_queue_maxsize,
            processing_pool_name=processing_pool_name,
            process_type=process_type,
        )

        logger.info("Running in PULL mode")

        self.client = TaskbrokerClient(
            hosts=broker_hosts,
            application=app.name,
            metrics=app.metrics,
            max_tasks_before_rebalance=rebalance_after,
            health_check_settings=(
                None
                if health_check_file_path is None
                else HealthCheckSettings(Path(health_check_file_path), health_check_sec_per_touch)
            ),
            rpc_secret=app.config["rpc_secret"],
            grpc_config=app.config["grpc_config"],
        )
        self._metrics = app.metrics

        self._grpc_sync_event = self._mp_context.Event()

        self._gettask_backoff_seconds = 0
        self._setstatus_backoff_seconds = 0

        self._processing_pool_name: str = processing_pool_name or "unknown"

    def start(self) -> int:
        """
        This starts a loop that runs until the worker completes its `max_task_count` or it is killed.
        """
        self.worker_pool.start_result_thread()
        self.worker_pool.start_spawn_children_thread()

        # Convert signals into KeyboardInterrupt.
        # Running shutdown() within the signal handler can lead to deadlocks
        def signal_handler(*args: Any) -> None:
            raise KeyboardInterrupt()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while True:
                self.run_once()
        except KeyboardInterrupt:
            self.shutdown()
            raise

    def run_once(self) -> None:
        """Access point for tests to run a single worker loop"""
        self._add_task()

    def _add_task(self) -> bool:
        """
        Add a task to child tasks queue. Returns False if no new task was fetched.
        """
        if self.worker_pool.is_worker_full():
            self._metrics.incr(
                "taskworker.worker.add_tasks.child_tasks_full",
                tags={"processing_pool": self._processing_pool_name},
            )
            # If we weren't able to add a task, backoff for a bit
            time.sleep(0.1)
            return False

        inflight = self.fetch_task()
        if inflight:
            return self.worker_pool.push_task(inflight)

        return False

    def _send_result(
        self, result: ProcessingResult, is_draining: bool = False
    ) -> InflightTaskActivation | None:
        """
        Send a result to the broker and conditionally fetch an additional task. Return a boolean indicating whether the result was sent successfully.
        """
        self._metrics.distribution(
            "taskworker.worker.complete_duration",
            time.monotonic() - result.receive_timestamp,
            tags={"processing_pool": self._processing_pool_name},
        )
        fetch_next = None if is_draining else FetchNextTask(namespace=self._namespace)
        next_task = self._send_update_task(result, fetch_next)
        return next_task

    def _send_update_task(
        self, result: ProcessingResult, fetch_next: FetchNextTask | None
    ) -> InflightTaskActivation | None:
        """
        Do the RPC call to this worker's taskbroker, and handle errors
        """
        logger.debug(
            "taskworker.workers._send_result",
            extra={
                "task_id": result.task_id,
                "next": fetch_next is not None,
                "processing_pool": self._processing_pool_name,
            },
        )

        self._grpc_sync_event.wait(self._setstatus_backoff_seconds)

        try:
            next_task = self.client.update_task(result, fetch_next)
            self._setstatus_backoff_seconds = 0
            return next_task
        except grpc.RpcError as e:
            self._setstatus_backoff_seconds = min(self._setstatus_backoff_seconds + 1, 10)
            logger.warning(
                "taskworker.send_update_task.failed",
                extra={"task_id": result.task_id, "error": e},
            )
            raise RequeueException(f"Failed to update task: {e}")
        except HostTemporarilyUnavailable as e:
            self._setstatus_backoff_seconds = min(
                self._setstatus_backoff_seconds + 4, MAX_BACKOFF_SECONDS_WHEN_HOST_UNAVAILABLE
            )
            logger.info(
                "taskworker.send_update_task.temporarily_unavailable",
                extra={"task_id": result.task_id, "error": str(e)},
            )
            raise RequeueException(f"Failed to update task: {e}")

    def fetch_task(self) -> InflightTaskActivation | None:
        self._grpc_sync_event.wait(self._gettask_backoff_seconds)
        try:
            activation = self.client.get_task(self._namespace)
        except grpc.RpcError as e:
            logger.info(
                "taskworker.fetch_task.failed",
                extra={"error": e, "processing_pool": self._processing_pool_name},
            )

            self._gettask_backoff_seconds = min(
                self._gettask_backoff_seconds + 4, MAX_BACKOFF_SECONDS_WHEN_HOST_UNAVAILABLE
            )
            return None

        if not activation:
            self._metrics.incr(
                "taskworker.worker.fetch_task.not_found",
                tags={"processing_pool": self._processing_pool_name},
            )
            logger.debug(
                "taskworker.fetch_task.not_found",
                extra={"processing_pool": self._processing_pool_name},
            )
            self._gettask_backoff_seconds = min(self._gettask_backoff_seconds + 1, 5)
            return None

        self._gettask_backoff_seconds = 0
        return activation

    def shutdown(self) -> None:
        """
        Shutdown the worker.
        """
        self._grpc_sync_event.set()
        self.worker_pool.shutdown()


class TaskWorkerProcessingPool:
    def __init__(
        self,
        app_module: str,
        # Here the bool is used to indicate whether this is a normal fetch or is being called
        # during shutdown.
        send_result_fn: Callable[[ProcessingResult, bool], InflightTaskActivation | None],
        mp_context: ForkContext | SpawnContext | ForkServerContext,
        max_child_task_count: int | None = None,
        concurrency: int = 1,
        child_tasks_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        result_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        processing_pool_name: str | None = None,
        pod_name: str | None = None,
        process_type: str = "spawn",
    ) -> None:
        self._concurrency = concurrency
        self._processing_pool_name = processing_pool_name or "unknown"
        self._pod_name = pod_name or "unknown"
        self._send_result = send_result_fn
        self._max_child_task_count = max_child_task_count
        self._app_module = app_module
        app = import_app(app_module)
        self._metrics = app.metrics

        self._mp_context = mp_context
        self._process_type = process_type

        self._child_tasks: multiprocessing.Queue[InflightTaskActivation] = self._mp_context.Queue(
            maxsize=child_tasks_queue_maxsize
        )
        self._processed_tasks: multiprocessing.Queue[ProcessingResult] = self._mp_context.Queue(
            maxsize=result_queue_maxsize
        )
        self._children: list[BaseProcess] = []
        self._shutdown_event = self._mp_context.Event()
        self._result_thread: threading.Thread | None = None
        self._spawn_children_thread: threading.Thread | None = None

    def send_result(self, result: ProcessingResult, is_draining: bool = False) -> None:
        """
        Call the passed in function. If is_draining is True, the function should not fetch a new task.
        That function should return:
        - An InflightTaskActivation if a new task was fetched
        - None if no new task was fetched
        - A RequeueException if the result failed to send and should be retried
        """
        try:
            worker_full = is_draining or self._child_tasks.full()
            next_task = self._send_result(result, worker_full)
            if next_task:
                self.push_task(next_task)
        except RequeueException:
            logger.warning("activation status couldn't be updated")
            # This can cause an infinite loop if we are draining and the result fails to send
            if not is_draining:
                self.put_result(result)

    def start_result_thread(self) -> None:
        """
        Start a thread that delivers results and fetches new tasks.
        We need to ship results in a thread because the RPC calls block for 20-50ms,
        and many tasks execute more quickly than that.

        Without additional threads, we end up publishing results too slowly
        and tasks accumulate in the `processed_tasks` queues and can cross
        their processing deadline.
        """

        def result_thread() -> None:
            logger.debug("taskworker.worker.result_thread.started")
            iopool = ThreadPoolExecutor(max_workers=self._concurrency)
            with iopool as executor:
                while not self._shutdown_event.is_set():
                    tags = {
                        "processing_pool": self._processing_pool_name,
                        "pod_name": self._pod_name,
                    }

                    try:
                        # 'qsize' is not implemented on all platforms, such as macOS
                        self._metrics.gauge(
                            "taskworker.child_tasks.size",
                            float(self._child_tasks.qsize()),
                            tags=tags,
                        )

                        self._metrics.gauge(
                            "taskworker.processed_tasks.size",
                            float(self._processed_tasks.qsize()),
                            tags=tags,
                        )
                    except Exception as e:
                        logger.debug(
                            "taskworker.worker.queue_gauges.error",
                            extra={"error": e, "processing_pool": self._processing_pool_name},
                        )

                    try:
                        result = self._processed_tasks.get(timeout=1.0)
                        executor.submit(self.send_result, result, False)
                    except queue.Empty:
                        self._metrics.incr(
                            "taskworker.worker.result_thread.queue_empty",
                            tags={"processing_pool": self._processing_pool_name},
                        )
                        continue

        self._result_thread = threading.Thread(
            name="send-result", target=result_thread, daemon=True
        )
        self._result_thread.start()

    def start_spawn_children_thread(self) -> None:
        def spawn_children_thread() -> None:
            logger.debug("taskworker.worker.spawn_children_thread.started")
            while not self._shutdown_event.is_set():
                self._children = [child for child in self._children if child.is_alive()]
                if len(self._children) >= self._concurrency:
                    time.sleep(0.1)
                    continue
                for i in range(self._concurrency - len(self._children)):
                    process = self._mp_context.Process(
                        name=f"taskworker-child-{i}",
                        target=child_process,
                        args=(
                            self._app_module,
                            self._child_tasks,
                            self._processed_tasks,
                            self._shutdown_event,
                            self._max_child_task_count,
                            self._processing_pool_name,
                            self._process_type,
                        ),
                    )
                    process.start()
                    self._children.append(process)
                    logger.info(
                        "taskworker.spawn_child",
                        extra={"pid": process.pid, "processing_pool": self._processing_pool_name},
                    )
                    self._metrics.incr(
                        "taskworker.worker.spawn_child",
                        tags={"processing_pool": self._processing_pool_name},
                    )

        self._spawn_children_thread = threading.Thread(
            name="spawn-children", target=spawn_children_thread, daemon=True
        )
        self._spawn_children_thread.start()

    def push_task(self, inflight: InflightTaskActivation, timeout: float | None = None) -> bool:
        """
        Push a task to child tasks queue.

        When timeout is `None`, blocks until the queue has space. When timeout is
        set (e.g. 5.0), waits at most that many seconds and returns `False` if the
        queue is still full (worker busy).
        """
        start_time = time.monotonic()
        try:
            self._child_tasks.put(inflight, timeout=timeout)
        except queue.Full:
            self._metrics.incr(
                "taskworker.worker.child_tasks.put.full",
                tags={"processing_pool": self._processing_pool_name},
            )
            logger.warning(
                "taskworker.add_task.child_task_queue_full",
                extra={
                    "task_id": inflight.activation.id,
                    "processing_pool": self._processing_pool_name,
                },
            )
            return False

        self._metrics.distribution(
            "taskworker.worker.child_task.put.duration",
            time.monotonic() - start_time,
            tags={"processing_pool": self._processing_pool_name},
        )
        return True

    def is_worker_full(self) -> bool:
        return self._child_tasks.full()

    def put_result(self, result: ProcessingResult) -> None:
        self._processed_tasks.put(result)

    def shutdown(self) -> None:
        """
        Shutdown cleanly
        Activate the shutdown event and drain results before terminating children.
        """
        logger.info("taskworker.worker.shutdown.start")
        self._shutdown_event.set()

        logger.info("taskworker.worker.shutdown.spawn_children")
        if self._spawn_children_thread:
            self._spawn_children_thread.join()

        logger.info("taskworker.worker.shutdown.children")
        for child in self._children:
            child.terminate()
        for child in self._children:
            child.join()

        logger.info("taskworker.worker.shutdown.result")
        if self._result_thread:
            # Use a timeout as sometimes this thread can deadlock on the Event.
            self._result_thread.join(timeout=5)

        # Drain any remaining results synchronously
        while True:
            try:
                result = self._processed_tasks.get_nowait()
                self.send_result(result, True)
            except queue.Empty:
                break

        logger.info("taskworker.worker.shutdown.complete")
