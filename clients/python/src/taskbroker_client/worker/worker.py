from __future__ import annotations

import ctypes
import logging
import multiprocessing
import os
import queue
import signal
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
from multiprocessing.process import BaseProcess
from multiprocessing.synchronize import Event
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Deque, Dict, List, Literal
from uuid import UUID, uuid4

import grpc
import prometheus_client
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from sentry_protos.taskbroker.v1 import taskbroker_pb2_grpc
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    FetchNextTask,
    PushTaskRequest,
    PushTaskResponse,
)

from taskbroker_client.app import import_app
from taskbroker_client.constants import (
    DEFAULT_GRPC_MAX_MESSAGE_SIZE,
    DEFAULT_REBALANCE_AFTER,
    DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
    DEFAULT_WORKER_QUEUE_SIZE,
    DEFAULT_WORKER_WARMUP_TIMEOUT_SEC,
    MAX_BACKOFF_SECONDS_WHEN_HOST_UNAVAILABLE,
    SHUTDOWN_POLL_INTERVAL_SEC,
    WORKER_CHILD_JOIN_TIMEOUT_SEC,
)
from taskbroker_client.metrics import MetricsBackend
from taskbroker_client.types import InflightTaskActivation, ProcessingResult
from taskbroker_client.worker.childtiming import (
    NO_SLOT,
    SLOT_WIDTH,
    ChildTimeAccounting,
    slot_count,
)
from taskbroker_client.worker.client import (
    HealthCheckSettings,
    HostTemporarilyUnavailable,
    RequestSignatureServerInterceptor,
    TaskbrokerClient,
    parse_rpc_secret_list,
)
from taskbroker_client.worker.push_clients import PushTaskbrokerClient
from taskbroker_client.worker.workerchild import ChildMessage, child_process

if TYPE_CHECKING:
    ServerInterceptor = grpc.ServerInterceptor[Any, Any]
else:
    ServerInterceptor = grpc.ServerInterceptor


logger = logging.getLogger(__name__)

WORKER_SERVICE_NAME = "sentry_protos.taskbroker.v1.WorkerService"


class ShutdownSignal:
    """
    Shutdown state that a signal handler is allowed to flip.

    Python runs signal handlers on the main thread in between bytecodes, which
    makes plain attribute assignment safe but anything that takes a lock unsafe:
    if the interrupted code already holds that lock, the handler blocks forever
    on the thread that would have released it. `threading.Event.set()` and
    `multiprocessing.Event.set()` both take a non-reentrant lock, so neither may
    be called from a handler. `request()` therefore only assigns a bool.

    The event is here so that a shutdown noticed on the main thread can wake
    sleeps on the result thread, and is only ever set from normal code.
    """

    def __init__(self) -> None:
        self._requested = False
        self._event = threading.Event()

    def request(self) -> None:
        """
        Ask for shutdown. This is the only method a signal handler may call.
        """
        self._requested = True

    def set(self) -> None:
        """
        Ask for shutdown and wake anything sleeping in `wait()`.

        Takes a lock, so this must never be called from a signal handler.
        """
        self._requested = True
        self._event.set()

    def is_set(self) -> bool:
        return self._requested

    def wait(self, timeout: float) -> bool:
        """
        Sleep up to `timeout` seconds, returning True if shutdown was requested.

        A handler can only flip the bool, so this polls rather than relying on
        the event alone.
        """
        deadline = time.monotonic() + timeout
        while True:
            if self._requested:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self._event.wait(min(remaining, SHUTDOWN_POLL_INTERVAL_SEC))


class WorkerPrometheusMetrics:
    """
    Owns the Prometheus registry, server, and metrics we expose for scraping.
    """

    def __init__(
        self, port: int, registry: prometheus_client.CollectorRegistry | None = None
    ) -> None:
        self.registry = registry or prometheus_client.CollectorRegistry()

        self.occupancy = prometheus_client.Gauge(
            "taskworker_worker_occupancy",
            "Fraction of worker child slots currently executing a task (busy / concurrency).",
            ["processing_pool"],
            registry=self.registry,
        )

        # Additive and unclamped: the scaler sums across pods and divides once.
        self.child_busy_seconds = prometheus_client.Counter(
            "taskworker_worker_child_busy_seconds",
            "Cumulative child-seconds spent executing tasks.",
            ["processing_pool"],
            registry=self.registry,
        )

        # What occupancy cannot express: free slots with nothing to do.
        self.child_wait_seconds = prometheus_client.Counter(
            "taskworker_worker_child_wait_seconds",
            "Cumulative child-seconds spent blocked waiting for a task to arrive.",
            ["processing_pool"],
            registry=self.registry,
        )

        prometheus_client.start_http_server(port, registry=self.registry)
        logger.info("taskworker.worker.prometheus_server_started", extra={"port": port})


class WorkerServicer(taskbroker_pb2_grpc.WorkerServiceServicer):
    """
    gRPC servicer that receives task activations pushed from the broker
    """

    def __init__(self, worker: TaskWorkerProcessingPool, push_task_timeout: float = 5) -> None:
        self.worker_pool = worker
        self.push_task_timeout = push_task_timeout

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

        # Push the task to the worker queue (wait at most N seconds)
        if not self.worker_pool.push_task(inflight, timeout=self.push_task_timeout):
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


ChildState = Literal["pending", "running", "exiting"]


@dataclass
class TrackedChild:
    process: BaseProcess
    state: ChildState
    release: Event
    # No default: an accountant with no slot silently measures nothing.
    timing: ChildTimeAccounting


class PushTaskWorker:
    _mp_context: ForkContext | SpawnContext | ForkServerContext

    def __init__(
        self,
        app_module: str,
        broker_service: str,
        max_child_task_count: int | None = None,
        namespace: str | None = None,
        concurrency: int = 1,
        min_concurrency: int = 0,
        child_tasks_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        result_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        rebalance_after: int = DEFAULT_REBALANCE_AFTER,
        processing_pool_name: str | None = None,
        pod_name: str | None = None,
        process_type: str = "spawn",
        health_check_file_path: str | None = None,
        health_check_sec_per_touch: float = DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
        grpc_port: int = 50052,
        grpc_max_message_size: int = -1,
        push_task_timeout: float = 5,
        skip_awaiting_futures: bool = True,
        warmup_timeout: float = DEFAULT_WORKER_WARMUP_TIMEOUT_SEC,
        prometheus_port: int | None = None,
        future_checking_frequency: float = 0.1,
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
            send_result_fn=self._send_results,
            max_child_task_count=max_child_task_count,
            concurrency=concurrency,
            min_concurrency=min_concurrency,
            child_tasks_queue_maxsize=child_tasks_queue_maxsize,
            result_queue_maxsize=result_queue_maxsize,
            processing_pool_name=processing_pool_name,
            pod_name=pod_name,
            process_type=process_type,
            update_in_batches=True,
            skip_awaiting_futures=skip_awaiting_futures,
            prometheus_port=prometheus_port,
            future_checking_frequency=future_checking_frequency,
        )

        logger.info("Running in PUSH mode")

        self.client = self._create_client(
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
            processing_pool_name=processing_pool_name,
        )
        self._metrics = app.metrics
        self._concurrency = concurrency
        self._shutdown_signal = ShutdownSignal()
        self._health_check_sec_per_touch = (
            None if health_check_file_path is None else health_check_sec_per_touch
        )
        self._health_check_stop_event = threading.Event()
        self._health_check_thread: threading.Thread | None = None

        self._setstatus_backoff_seconds = 0

        self._processing_pool_name: str = processing_pool_name or "unknown"

        self._grpc_port = grpc_port
        self._grpc_max_message_size = grpc_max_message_size
        self._grpc_secrets = parse_rpc_secret_list(app.config["rpc_secret"])
        self._push_task_timeout = push_task_timeout

        self._warmup_timeout = warmup_timeout

    def _create_client(
        self,
        service: str,
        application: str,
        metrics: MetricsBackend,
        health_check_settings: HealthCheckSettings | None = None,
        rpc_secret: str | None = None,
        grpc_config: str | None = None,
        processing_pool_name: str | None = None,
    ) -> PushTaskbrokerClient:
        return PushTaskbrokerClient(
            service=service,
            application=application,
            metrics=metrics,
            health_check_settings=health_check_settings,
            rpc_secret=rpc_secret,
            grpc_config=grpc_config,
            processing_pool_name=processing_pool_name,
        )

    def _send_results(
        self, results: list[ProcessingResult], is_draining: bool = False
    ) -> InflightTaskActivation | None:
        """
        Send a result to the broker. If the set has failed before, sleep briefly before retrying.
        """
        for result in results:
            self._metrics.distribution(
                "taskworker.worker.complete_duration",
                time.monotonic() - result.receive_timestamp,
                tags={"processing_pool": self._processing_pool_name},
            )
        self._metrics.distribution(
            "taskworker.worker.update_status_batch_size",
            len(results),
            tags={"processing_pool": self._processing_pool_name},
        )

        logger.debug(
            "taskworker.send_update_task_batch.batch_sent",
            extra={
                "results": [result.task_id for result in results],
                "processing_pool": self._processing_pool_name,
            },
        )
        # Use the shutdown signal as a sleep mechanism
        self._shutdown_signal.wait(self._setstatus_backoff_seconds)

        try:
            self.client.update_tasks(results)
            self._setstatus_backoff_seconds = 0
            return None
        except grpc.RpcError as e:
            self._setstatus_backoff_seconds = min(self._setstatus_backoff_seconds + 1, 10)
            logger.warning(
                "taskworker.send_update_task_batch.failed",
                extra={"results": [result.task_id for result in results], "error": e},
            )
            if e.code() != grpc.StatusCode.NOT_FOUND:
                # If the task was not found, we can't update it, so we should just return None
                raise RequeueException(f"Failed to update task batch: {e}")
        except HostTemporarilyUnavailable as e:
            self._setstatus_backoff_seconds = min(
                self._setstatus_backoff_seconds + 4, MAX_BACKOFF_SECONDS_WHEN_HOST_UNAVAILABLE
            )
            logger.info(
                "taskworker.send_update_task_batch.temporarily_unavailable",
                extra={"task_ids": [result.task_id for result in results], "error": str(e)},
            )
            raise RequeueException(f"Failed to update task: {e}")

        return None

    def _start_health_check_thread(self) -> None:
        if self._health_check_sec_per_touch is None:
            return
        if self._health_check_thread is not None and self._health_check_thread.is_alive():
            return

        health_check_sec_per_touch = self._health_check_sec_per_touch
        self._health_check_stop_event.clear()

        def health_check_thread() -> None:
            logger.debug("taskworker.worker.health_check_thread.started")
            while not self._health_check_stop_event.is_set():
                try:
                    self.client.emit_health_check()
                except Exception as e:
                    logger.warning(
                        "taskworker.worker.health_check.failed",
                        extra={
                            "error": e,
                            "processing_pool": self._processing_pool_name,
                        },
                    )

                self._health_check_stop_event.wait(health_check_sec_per_touch)

        self._health_check_thread = threading.Thread(
            name="push-health-check", target=health_check_thread, daemon=True
        )
        self._health_check_thread.start()

    def _stop_health_check_thread(self) -> None:
        self._health_check_stop_event.set()
        if self._health_check_thread is not None:
            self._health_check_thread.join(timeout=5)
            self._health_check_thread = None

    def _await_children_warm(self) -> None:
        """
        Block until all children have warmed up or warmup_timeout elapses.

        On timeout we fall through and serve anyway, a degraded-but-routable pod
        beats one that never becomes ready.
        """
        required = self._concurrency
        if required <= 0:
            return

        warmup_start = time.monotonic()
        deadline = warmup_start + self._warmup_timeout
        timed_out = False
        while self.worker_pool.ready_count < required:
            if time.monotonic() >= deadline:
                timed_out = True
                self._metrics.incr(
                    "taskworker.worker.warmup_timeout",
                    tags={"processing_pool": self._processing_pool_name},
                )
                logger.warning(
                    "taskworker.worker.warmup_timeout",
                    extra={
                        "processing_pool": self._processing_pool_name,
                        "ready_count": self.worker_pool.ready_count,
                        "required": required,
                        "warmup_timeout": self._warmup_timeout,
                    },
                )
                break
            # Sleep and break early if shutdown was requested.
            if self._shutdown_signal.wait(0.25):
                break

        self._metrics.distribution(
            "taskworker.worker.warmup_duration",
            time.monotonic() - warmup_start,
            tags={"processing_pool": self._processing_pool_name},
        )
        logger.info(
            "taskworker.worker.warmup_complete",
            extra={
                "processing_pool": self._processing_pool_name,
                "ready_count": self.worker_pool.ready_count,
                "required": required,
                "timed_out": timed_out,
            },
        )

    def start(self) -> int:
        """
        This starts the worker gRPC server.
        """
        self.worker_pool.start_metrics_thread()
        self.worker_pool.start_result_thread()
        self.worker_pool.start_spawn_children_thread()

        server: grpc.Server | None = None
        server_started = False
        health_servicer: health.HealthServicer | None = None

        # Record the request and let the loop below act on it. Raising from a
        # handler unwinds at an arbitrary bytecode and can leave the locks held
        # by the interrupted code in a broken state; calling anything that takes
        # a lock (server.stop(), Event.set()) can deadlock against the code the
        # handler interrupted. See ShutdownSignal.
        def signal_handler(*args: Any) -> None:
            self._shutdown_signal.request()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            # Start gRPC server
            interceptors: List[ServerInterceptor] = []

            if self._grpc_secrets:
                interceptors = [RequestSignatureServerInterceptor(self._grpc_secrets)]

            max_message_size = (
                self._grpc_max_message_size
                if self._grpc_max_message_size > 0
                else int(
                    os.environ.get(
                        "TASKBROKER_GRPC_MAX_MESSAGE_SIZE", DEFAULT_GRPC_MAX_MESSAGE_SIZE
                    )
                )
            )
            server = grpc.server(
                ThreadPoolExecutor(max_workers=self._concurrency),
                interceptors=interceptors,
                options=[
                    ("grpc.max_receive_message_length", max_message_size),
                    ("grpc.max_send_message_length", max_message_size),
                ],
            )

            taskbroker_pb2_grpc.add_WorkerServiceServicer_to_server(
                WorkerServicer(self.worker_pool, self._push_task_timeout), server
            )

            # The health service is used by the K8s readiness check
            health_servicer = health.HealthServicer()
            health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
            health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
            health_servicer.set(WORKER_SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING)

            server.add_insecure_port(f"[::]:{self._grpc_port}")

            # Don't accept connections we are about to drop.
            if self._shutdown_signal.is_set():
                return 0

            server.start()
            server_started = True

            # Hold NOT_SERVING until children are warm so the pod stays out of
            # the NEG/readiness set while its child processes are still loading.
            self._await_children_warm()

            # If shutdown was requested during warmup, don't advertise SERVING.
            # Bail to the finally below, which sets NOT_SERVING and tears everything down.
            if self._shutdown_signal.is_set():
                return 0

            # Indicate that the server is ready
            health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
            health_servicer.set(WORKER_SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)

            logger.info("taskworker.grpc_server.started", extra={"port": self._grpc_port})
            self._start_health_check_thread()

            # Poll so a signal handler that only flipped a bool still gets us
            # out, while also noticing a server that terminated on its own.
            #
            # Mind the return value of `wait_for_termination(timeout=...)`: it
            # is True when the *timeout* elapsed, i.e. while the server is still
            # healthy, and False once the server has terminated. That is the
            # inverse of `Event.wait()`, and reading it as "has terminated" is
            # what made a previous version of this patch exit half a second
            # after startup and take down every worker.
            while not self._shutdown_signal.is_set():
                still_running = server.wait_for_termination(timeout=SHUTDOWN_POLL_INTERVAL_SEC)
                if not still_running:
                    logger.warning("taskworker.grpc_server.terminated_unexpectedly")
                    break

        finally:
            if health_servicer is not None:
                health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
                health_servicer.set(WORKER_SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING)

            if server is not None and server_started:
                server.stop(grace=5)

            self.shutdown()

        return 0

    def shutdown(self) -> None:
        """
        Shutdown the worker.
        """
        self._stop_health_check_thread()
        self._shutdown_signal.set()
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
        min_concurrency: int = 0,
        child_tasks_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        result_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        rebalance_after: int = DEFAULT_REBALANCE_AFTER,
        processing_pool_name: str | None = None,
        process_type: str = "spawn",
        health_check_file_path: str | None = None,
        health_check_sec_per_touch: float = DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
        skip_awaiting_futures: bool = True,
        future_checking_frequency: float = 0.1,
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
            send_result_fn=self._send_results,
            max_child_task_count=max_child_task_count,
            concurrency=concurrency,
            min_concurrency=min_concurrency,
            child_tasks_queue_maxsize=child_tasks_queue_maxsize,
            result_queue_maxsize=result_queue_maxsize,
            processing_pool_name=processing_pool_name,
            process_type=process_type,
            update_in_batches=False,
            skip_awaiting_futures=skip_awaiting_futures,
            future_checking_frequency=future_checking_frequency,
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
            processing_pool_name=processing_pool_name,
        )
        self._metrics = app.metrics

        self._shutdown_signal = ShutdownSignal()

        self._gettask_backoff_seconds = 0
        self._setstatus_backoff_seconds = 0

        self._processing_pool_name: str = processing_pool_name or "unknown"

    def start(self) -> int:
        """
        This starts a loop that runs until the worker completes its `max_task_count` or it is killed.
        """
        self.worker_pool.start_metrics_thread()
        self.worker_pool.start_result_thread()
        self.worker_pool.start_spawn_children_thread()

        # Record the request and let the loop below act on it. Raising from a
        # handler unwinds at an arbitrary bytecode and can leave the locks held
        # by the interrupted code in a broken state; calling anything that takes
        # a lock (Event.set()) can deadlock against the code the handler
        # interrupted. See ShutdownSignal.
        def signal_handler(*args: Any) -> None:
            self._shutdown_signal.request()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while not self._shutdown_signal.is_set():
                self.run_once()
        finally:
            self.shutdown()

        return 0

    def run_once(self) -> None:
        """Access point for tests to run a single worker loop"""
        self._add_task()

    def _add_task(self) -> bool:
        """
        Add a task to child tasks queue. Returns False if no new task was fetched.
        """
        if self.worker_pool.is_worker_full():
            self.client.emit_health_check()
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

    def _send_results(
        self, results: list[ProcessingResult], is_draining: bool = False
    ) -> InflightTaskActivation | None:
        """
        Send a result to the broker and conditionally fetch an additional task. Return a boolean indicating whether the result was sent successfully.
        """
        assert (
            len(results) == 1
        ), "Only one result can be sent at a time with the regular pull client"
        self._metrics.distribution(
            "taskworker.worker.complete_duration",
            time.monotonic() - results[0].receive_timestamp,
            tags={"processing_pool": self._processing_pool_name},
        )
        fetch_next = None if is_draining else FetchNextTask(namespace=self._namespace)
        next_task = self._send_update_task(results[0], fetch_next)
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

        if self._shutdown_signal.wait(self._setstatus_backoff_seconds):
            # Don't claim a task we won't be around to run.
            fetch_next = None

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
        if self._shutdown_signal.wait(self._gettask_backoff_seconds):
            return None

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

        # get_task() blocks with no deadline, so a SIGTERM can arrive while it
        # is in flight. Re-check before handing the activation to a child:
        # claiming work we are not going to run means waiting for it to expire
        # on the broker before anyone else picks it up.
        if self._shutdown_signal.is_set():
            self._metrics.incr(
                "taskworker.worker.fetch_task.dropped_during_shutdown",
                tags={"processing_pool": self._processing_pool_name},
            )
            logger.info(
                "taskworker.fetch_task.dropped_during_shutdown",
                extra={
                    "task_id": activation.activation.id,
                    "processing_pool": self._processing_pool_name,
                },
            )
            return None

        return activation

    def shutdown(self) -> None:
        """
        Shutdown the worker.
        """
        self._shutdown_signal.set()
        self.worker_pool.shutdown()


class TaskWorkerProcessingPool:
    def __init__(
        self,
        app_module: str,
        # Here the bool is used to indicate whether this is a normal fetch or is being called
        # during shutdown.
        send_result_fn: Callable[[list[ProcessingResult], bool], InflightTaskActivation | None],
        mp_context: ForkContext | SpawnContext | ForkServerContext,
        max_child_task_count: int | None = None,
        concurrency: int = 1,
        min_concurrency: int = 0,
        child_tasks_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        result_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        processing_pool_name: str | None = None,
        pod_name: str | None = None,
        process_type: str = "spawn",
        update_in_batches: bool = False,
        skip_awaiting_futures: bool = True,
        prometheus_port: int | None = None,
        future_checking_frequency: float = 0.1,
    ) -> None:
        self._concurrency = concurrency

        if min_concurrency < concurrency:
            self._min_concurrency = min_concurrency
        else:
            raise ValueError("Minimum concurrency must be strictly below concurrency")

        self._processing_pool_name = processing_pool_name or "unknown"
        self._pod_name = pod_name or "unknown"

        self._send_result_fn = send_result_fn

        self._max_child_task_count = max_child_task_count
        self._app_module = app_module
        app = import_app(app_module)
        self._metrics = app.metrics
        self._skip_awaiting_futures = skip_awaiting_futures
        self._future_checking_frequency = future_checking_frequency
        self._update_in_batches = update_in_batches
        self._mp_context = mp_context
        self._process_type = process_type

        self._child_tasks: multiprocessing.Queue[InflightTaskActivation] = self._mp_context.Queue(
            maxsize=child_tasks_queue_maxsize
        )
        self._processed_tasks: multiprocessing.Queue[ProcessingResult] = self._mp_context.Queue(
            maxsize=result_queue_maxsize
        )
        self._children: Dict[UUID, TrackedChild] = {}
        self._exiting_children: Deque[UUID] = deque()

        # Two generations: unreaped exiting children overlap their replacements.
        self._timing_slots: int = slot_count(concurrency)
        self._timing_shm: ctypes.Array[ctypes.c_double] = self._mp_context.RawArray(
            "d", SLOT_WIDTH * self._timing_slots
        )
        self._free_timing_slots: Deque[int] = deque(range(self._timing_slots))
        self._children_lock = threading.Lock()
        self._shutdown_event = self._mp_context.Event()
        self._prometheus_port = prometheus_port
        self._prom: WorkerPrometheusMetrics | None = None
        self._result_thread: threading.Thread | None = None
        self._metrics_thread: threading.Thread | None = None
        self._spawn_children_thread: threading.Thread | None = None

    def _acquire_timing_slot(self) -> int:
        """Take a zeroed shared-memory slot for a new child.

        `NO_SLOT` means the pool ran out, which the two-generation sizing should
        make impossible. That child is then left out of both the occupancy
        numerator and its divisor, so occupancy stays honest over the children
        that are measured and the metric says the sizing was wrong.
        """
        with self._children_lock:
            slot = self._free_timing_slots.popleft() if self._free_timing_slots else NO_SLOT

        if slot == NO_SLOT:
            logger.error(
                "taskworker.worker.child.timing_slot_exhausted",
                extra={
                    "slots": self._timing_slots,
                    "processing_pool": self._processing_pool_name,
                },
            )
            self._metrics.incr(
                "taskworker.worker.child.timing_slot_exhausted",
                tags={"processing_pool": self._processing_pool_name},
            )
            return NO_SLOT

        # Zero on the way out, not on release: a released child can still write
        # once before it breaks out of its loop.
        base = slot * SLOT_WIDTH
        for offset in range(SLOT_WIDTH):
            self._timing_shm[base + offset] = 0.0

        return slot

    def _release_timing_slot(self, slot: int) -> None:
        """Return a slot whose child never started. The reap path appends
        directly, since it already holds `_children_lock`."""
        if slot == NO_SLOT:
            return

        with self._children_lock:
            self._free_timing_slots.append(slot)

    @property
    def ready_count(self) -> int:
        """Number of children that have finished warming up and are consuming."""
        with self._children_lock:
            return sum(1 for c in self._children.values() if c.state == "running")

    def _accounting_log(
        self, busy_time: float, wait_time: float, ceiling: float, running_count: int
    ) -> dict[str, float | int | str]:
        return {
            "busy_time": busy_time,
            "wait_time": wait_time,
            "ceiling": ceiling,
            "running_count": running_count,
            "processing_pool": self._processing_pool_name,
        }

    def _emit_periodic_metrics(self) -> None:
        tags = {
            "processing_pool": self._processing_pool_name,
            "pod_name": self._pod_name,
        }

        # Emit queue size metrics
        try:
            # Method 'qsize' not implemented on all platforms, such as macOS
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

        with self._children_lock:
            state_counts: dict[ChildState, int] = {
                "pending": 0,
                "running": 0,
                "exiting": 0,
            }

            busy_time = 0.0
            wait_time = 0.0
            eligible_time = 0.0
            accounted_running = 0
            for child in self._children.values():
                state_counts[child.state] += 1

                # Running only: the numerator must match occupancy's divisor.
                if child.state != "running":
                    continue

                # A slotless child reports nothing; counting it would read as idle.
                if child.timing.slot == NO_SLOT:
                    continue

                accounted_running += 1
                result = child.timing.sample(time.monotonic())
                busy_time += result.busy
                wait_time += result.wait
                eligible_time += result.eligible

            exiting_children = len(self._exiting_children)

        # Emitted during warmup too: zero is correct for a counter.
        self._metrics.distribution(
            "taskworker.worker.child_busy_seconds",
            busy_time,
            tags=tags,
        )
        self._metrics.distribution(
            "taskworker.worker.child_wait_seconds",
            wait_time,
            tags=tags,
        )
        if self._prom is not None:
            # inc(0.0) registers the series, so a new pod reads idle not missing.
            self._prom.child_busy_seconds.labels(processing_pool=self._processing_pool_name).inc(
                max(0.0, busy_time)
            )
            self._prom.child_wait_seconds.labels(processing_pool=self._processing_pool_name).inc(
                max(0.0, wait_time)
            )

        # Children the loop above skipped are absent here too, so occupancy is a
        # ratio over the measured set. The `children` gauge still counts them all.
        running_count = accounted_running
        if running_count > 0 and eligible_time > 0:
            # Sum of the windows each child was measured over. `elapsed *
            # running_count` would instead bill a child baselined part-way
            # through this flush as if it had been here all along, which reads
            # as idle time the pool never had.
            ceiling = eligible_time
            # busy and wait partition every measured window, so the pair must
            # land on the ceiling. Over means time was counted twice; under
            # means a child's totals went backwards and time was dropped. Both
            # should stay at zero, and occupancy is not trustworthy while either
            # is firing.
            if busy_time > ceiling or wait_time > ceiling:
                self._metrics.incr(
                    "taskworker.worker.occupancy.accounting_overflow",
                    tags=tags,
                )
                logger.warning(
                    "taskworker.worker.occupancy.accounting_overflow",
                    extra=self._accounting_log(busy_time, wait_time, ceiling, running_count),
                )
            elif busy_time + wait_time < ceiling * 0.9:
                self._metrics.incr(
                    "taskworker.worker.occupancy.accounting_deficit",
                    tags=tags,
                )
                logger.warning(
                    "taskworker.worker.occupancy.accounting_deficit",
                    extra=self._accounting_log(busy_time, wait_time, ceiling, running_count),
                )

            occupancy = min(busy_time / ceiling, 1.0)
            self._metrics.gauge(
                "taskworker.worker.occupancy",
                occupancy,
                tags=tags,
            )
            if self._prom is not None:
                self._prom.occupancy.labels(processing_pool=self._processing_pool_name).set(
                    occupancy
                )

        self._metrics.gauge(
            "taskworker.worker.concurrency",
            float(self._concurrency),
            tags=tags,
        )

        # Emit number of children in each state
        for state, count in state_counts.items():
            self._metrics.gauge(
                "taskworker.worker.children",
                float(count),
                tags={**tags, "state": state},
            )

        # Emit number of children waiting to exit
        self._metrics.gauge(
            "taskworker.worker.exiting_children.size",
            float(exiting_children),
            tags=tags,
        )

    def send_results(self, results: list[ProcessingResult], is_draining: bool = False) -> None:
        """
        Call the passed in function. If is_draining is True, the function should not fetch a new task.
        That function should return:
        - An InflightTaskActivation if a new task was fetched
        - None if no new task was fetched
        - A RequeueException if the result failed to send and should be retried
        """
        try:
            worker_full = is_draining or self._child_tasks.full()
            next_task = self._send_result_fn(results, worker_full)
            if next_task:
                self.push_task(next_task)
        except RequeueException:
            logger.warning("activation status couldn't be updated")
            # This can cause an infinite loop if we are draining and the result fails to send
            if not is_draining:
                for result in results:
                    self.put_result(result)

    def start_metrics_thread(self) -> None:
        """
        Start a thread that emits metrics on an interval.
        """
        if self._prometheus_port is not None and self._prom is None:
            self._prom = WorkerPrometheusMetrics(self._prometheus_port)

        def metrics_thread() -> None:
            while True:
                try:
                    self._emit_periodic_metrics()
                except Exception as e:
                    logger.debug(
                        "taskworker.worker.queue_gauges.error",
                        extra={"error": e, "processing_pool": self._processing_pool_name},
                    )

                time.sleep(1)

        self._metrics_thread = threading.Thread(name="metrics", target=metrics_thread, daemon=True)

        self._metrics_thread.start()

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
                    results = []
                    while True:
                        try:
                            result = self._processed_tasks.get(timeout=1.0)
                            if not self._update_in_batches:
                                # This needs to stay until the pull taskbroker is removed
                                executor.submit(self.send_results, [result], False)
                                break
                            else:
                                results.append(result)
                                if len(results) >= self._concurrency:
                                    executor.submit(self.send_results, results, False)
                                    results = []
                        except queue.Empty:
                            if not results:
                                # Only increment if there was nothing in the queue at all
                                self._metrics.incr(
                                    "taskworker.worker.result_thread.queue_empty",
                                    tags={"processing_pool": self._processing_pool_name},
                                )
                            elif self._update_in_batches:
                                executor.submit(self.send_results, results, False)
                                results = []
                            break

        self._result_thread = threading.Thread(
            name="send-result", target=result_thread, daemon=True
        )
        self._result_thread.start()

    def start_spawn_children_thread(self) -> None:
        def spawn_children_thread() -> None:
            logger.debug("taskworker.worker.spawn_children_thread.started")

            # Queue of incoming message from children
            messages: multiprocessing.Queue[ChildMessage] = self._mp_context.Queue()

            while not self._shutdown_event.is_set():
                # Read any events that may have come in since the last loop iteration
                received: List[ChildMessage] = []

                while True:
                    try:
                        message = messages.get(block=False)
                        received.append(message)
                    except queue.Empty:
                        break

                # Lifecycle-queue lag. A rising line means this thread is behind.
                if received:
                    drain_at = time.monotonic()
                    self._metrics.distribution(
                        "taskworker.worker.child_message.age",
                        drain_at - min(m.timestamp for m in received),
                        tags={
                            "processing_pool": self._processing_pool_name,
                            "pod_name": self._pod_name,
                        },
                    )

                with self._children_lock:
                    children = list(self._children.items())

                    for cid, c in children:
                        if c.process.is_alive():
                            continue

                        c.process.join(timeout=0)
                        self._children.pop(cid)

                        # Not at `exiting`: a released child can still publish once.
                        if c.timing.slot != NO_SLOT:
                            self._free_timing_slots.append(c.timing.slot)

                        logger.info(
                            "taskworker.child.exited",
                            extra={
                                "pid": c.process.pid,
                                "cid": str(cid),
                                "exitcode": c.process.exitcode,
                                "state": c.state,
                                "processing_pool": self._processing_pool_name,
                            },
                        )

                    for message in received:
                        child = self._children.get(message.child_id)

                        # If we received a message from a child, we MUST be tracking that child
                        if child is None:
                            logger.warning(
                                "taskworker.child_message.unknown_child",
                                extra={
                                    "cid": str(message.child_id),
                                    "event": message.event,
                                    "processing_pool": self._processing_pool_name,
                                },
                            )

                            continue

                        # Baseline here, where it also enters `running_count`.
                        if message.event == "running":
                            child.state = "running"
                            child.timing.mark_running(time.monotonic())

                        # This child wants to exit, but we may not have enough running children to shut down right away
                        elif message.event == "exiting":
                            self._exiting_children.append(message.child_id)

                    while True:
                        # Compute how many children are still running
                        running = sum(1 for c in self._children.values() if c.state == "running")

                        if running <= self._min_concurrency:
                            # Cannot shut down any more children without falling below minimum concurrency (guaranteed < concurrency)
                            break

                        if not self._exiting_children:
                            # No children are waiting to exit
                            break

                        child_id = self._exiting_children.popleft()
                        child = self._children.get(child_id)

                        # Child may have died already
                        if child is None:
                            continue

                        child.state = "exiting"
                        child.timing.mark_stopped()
                        child.release.set()

                    spawned = sum(1 for c in self._children.values() if c.state != "exiting")

                # How many children do we need to spawn?
                needed = max(self._concurrency - spawned, 0)

                for _ in range(needed):
                    child_id = uuid4()
                    release = self._mp_context.Event()
                    timing_slot = self._acquire_timing_slot()

                    process = self._mp_context.Process(
                        name=f"taskworker-child-{child_id}",
                        target=child_process,
                        args=(
                            child_id,
                            self._app_module,
                            self._child_tasks,
                            self._processed_tasks,
                            self._shutdown_event,
                            self._max_child_task_count,
                            self._processing_pool_name,
                            self._process_type,
                            self._skip_awaiting_futures,
                            self._future_checking_frequency,
                            messages,
                            release,
                            self._timing_shm,
                            timing_slot,
                        ),
                    )

                    try:
                        process.start()

                        with self._children_lock:
                            child = TrackedChild(
                                process=process,
                                state="pending",
                                release=release,
                                timing=ChildTimeAccounting(shm=self._timing_shm, slot=timing_slot),
                            )

                            self._children[child_id] = child
                    except Exception as e:
                        # Never came up, so nothing will write to its slot.
                        self._release_timing_slot(timing_slot)

                        logger.exception(
                            "taskworker.child.spawn.failed",
                            extra={
                                "cid": str(child_id),
                                "error": e,
                                "processing_pool": self._processing_pool_name,
                            },
                        )

                        self._metrics.incr(
                            "taskworker.worker.child.spawn",
                            tags={
                                "processing_pool": self._processing_pool_name,
                                "result": "failure",
                            },
                        )

                        continue

                    logger.info(
                        "taskworker.spawn_child",
                        extra={
                            "pid": process.pid,
                            "cid": str(child_id),
                            "processing_pool": self._processing_pool_name,
                        },
                    )

                    self._metrics.incr(
                        "taskworker.worker.spawn_child",
                        tags={
                            "processing_pool": self._processing_pool_name,
                            "result": "success",
                        },
                    )

                time.sleep(0.1)

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
        with self._children_lock:
            children = [tracked_child.process for tracked_child in self._children.values()]

        for child in children:
            child.terminate()
        for child in children:
            child.join(WORKER_CHILD_JOIN_TIMEOUT_SEC)
            if child.is_alive():
                child.kill()
                child.join()

        logger.info("taskworker.worker.shutdown.result")
        if self._result_thread:
            # Use a timeout as sometimes this thread can deadlock on the Event.
            self._result_thread.join(timeout=5)

        # Drain any remaining results synchronously
        while True:
            try:
                result = self._processed_tasks.get_nowait()
                self.send_results([result], True)
            except queue.Empty:
                break

        logger.info("taskworker.worker.shutdown.complete")
