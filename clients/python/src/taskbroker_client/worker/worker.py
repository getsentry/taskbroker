from __future__ import annotations

import logging
import multiprocessing
import os
import queue
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
from multiprocessing.process import BaseProcess
from multiprocessing.synchronize import Event
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal
from uuid import UUID, uuid4

import grpc
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
    WORKER_CHILD_JOIN_TIMEOUT_SEC,
)
from taskbroker_client.metrics import MetricsBackend
from taskbroker_client.types import InflightTaskActivation, ProcessingResult
from taskbroker_client.worker.client import (
    HealthCheckSettings,
    HostTemporarilyUnavailable,
    RequestSignatureServerInterceptor,
    TaskbrokerClient,
    parse_rpc_secret_list,
)
from taskbroker_client.worker.push_clients import BatchPushTaskbrokerClient, PushTaskbrokerClient
from taskbroker_client.worker.workerchild import ChildMessage, child_process

if TYPE_CHECKING:
    ServerInterceptor = grpc.ServerInterceptor[Any, Any]
else:
    ServerInterceptor = grpc.ServerInterceptor


logger = logging.getLogger(__name__)

WORKER_SERVICE_NAME = "sentry_protos.taskbroker.v1.WorkerService"


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


ChildState = Literal["pending", "ready", "exiting"]


@dataclass
class TrackedChild:
    process: BaseProcess
    state: ChildState
    release: Event


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
        push_task_timeout: float = 5,
        update_in_batches: bool = False,
        skip_awaiting_futures: bool = True,
        warmup_timeout: float = DEFAULT_WORKER_WARMUP_TIMEOUT_SEC,
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
            child_tasks_queue_maxsize=child_tasks_queue_maxsize,
            result_queue_maxsize=result_queue_maxsize,
            processing_pool_name=processing_pool_name,
            pod_name=pod_name,
            process_type=process_type,
            update_in_batches=update_in_batches,
            skip_awaiting_futures=skip_awaiting_futures,
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
        self._grpc_sync_event = self._mp_context.Event()
        self._health_check_sec_per_touch = (
            None if health_check_file_path is None else health_check_sec_per_touch
        )
        self._health_check_stop_event = threading.Event()
        self._health_check_thread: threading.Thread | None = None

        self._setstatus_backoff_seconds = 0

        self._processing_pool_name: str = processing_pool_name or "unknown"

        self._grpc_port = grpc_port
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
        assert (
            len(results) == 1
        ), "Only one result can be sent at a time with the regular push client"
        result = results[0]
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
            self.client.update_tasks([result])
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
            # Sleep and break early if shutdown was requested via shutdown().
            if self._grpc_sync_event.wait(0.25):
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

        # Convert signals into KeyboardInterrupt.
        # Running shutdown() within the signal handler can lead to deadlocks

        server: grpc.Server | None = None
        health_servicer: health.HealthServicer | None = None

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

            max_message_size = int(
                os.environ.get("TASKBROKER_GRPC_MAX_MESSAGE_SIZE", DEFAULT_GRPC_MAX_MESSAGE_SIZE)
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
            server.start()

            # Hold NOT_SERVING until children are warm so the pod stays out of
            # the NEG/readiness set while its child processes are still loading.
            self._await_children_warm()

            # If shutdown was requested during warmup, don't advertise SERVING.
            # Bail to the finally below, which sets NOT_SERVING and tears everything down.
            if self._grpc_sync_event.is_set():
                return 0

            # Indicate that the server is ready
            health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
            health_servicer.set(WORKER_SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)

            logger.info("taskworker.grpc_server.started", extra={"port": self._grpc_port})
            self._start_health_check_thread()

            try:
                server.wait_for_termination()
            except KeyboardInterrupt:
                # Signals are converted to KeyboardInterrupt, swallow for exit code 0
                pass

        finally:
            if health_servicer is not None:
                health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
                health_servicer.set(WORKER_SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING)

            if server is not None:
                server.stop(grace=5)

            self._stop_health_check_thread()
            self.worker_pool.shutdown()

        return 0

    def shutdown(self) -> None:
        """
        Shutdown the worker.
        """
        self._stop_health_check_thread()
        self._grpc_sync_event.set()
        self.worker_pool.shutdown()


class BatchPushTaskWorker(PushTaskWorker):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        assert (
            kwargs["update_in_batches"] is True
        ), "BatchPushTaskWorker must be initialized with update_in_batches=True"
        super().__init__(*args, **kwargs)

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
        return BatchPushTaskbrokerClient(
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
        # Use the shutdown_event as a sleep mechanism
        self._grpc_sync_event.wait(self._setstatus_backoff_seconds)

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
        skip_awaiting_futures: bool = True,
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
            child_tasks_queue_maxsize=child_tasks_queue_maxsize,
            result_queue_maxsize=result_queue_maxsize,
            processing_pool_name=processing_pool_name,
            process_type=process_type,
            skip_awaiting_futures=skip_awaiting_futures,
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

        self._grpc_sync_event = self._mp_context.Event()

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
        send_result_fn: Callable[[list[ProcessingResult], bool], InflightTaskActivation | None],
        mp_context: ForkContext | SpawnContext | ForkServerContext,
        max_child_task_count: int | None = None,
        concurrency: int = 1,
        child_tasks_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        result_queue_maxsize: int = DEFAULT_WORKER_QUEUE_SIZE,
        processing_pool_name: str | None = None,
        pod_name: str | None = None,
        process_type: str = "spawn",
        update_in_batches: bool = False,
        skip_awaiting_futures: bool = True,
    ) -> None:
        self._concurrency = concurrency
        self._processing_pool_name = processing_pool_name or "unknown"
        self._pod_name = pod_name or "unknown"
        self._update_in_batches = update_in_batches

        self._send_result_fn = send_result_fn

        self._max_child_task_count = max_child_task_count
        self._app_module = app_module
        app = import_app(app_module)
        self._metrics = app.metrics
        self._skip_awaiting_futures = skip_awaiting_futures

        self._mp_context = mp_context
        self._process_type = process_type

        self._child_tasks: multiprocessing.Queue[InflightTaskActivation] = self._mp_context.Queue(
            maxsize=child_tasks_queue_maxsize
        )
        self._processed_tasks: multiprocessing.Queue[ProcessingResult] = self._mp_context.Queue(
            maxsize=result_queue_maxsize
        )
        self._children: Dict[UUID, TrackedChild] = {}
        self._children_lock = threading.Lock()
        self._shutdown_event = self._mp_context.Event()
        self._result_thread: threading.Thread | None = None
        self._metrics_thread: threading.Thread | None = None
        self._spawn_children_thread: threading.Thread | None = None

    @property
    def ready_count(self) -> int:
        """Number of children that have finished warming up and are consuming."""
        with self._children_lock:
            return sum(1 for c in self._children.values() if c.state == "ready")

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

        def metrics_thread() -> None:
            tags = {
                "processing_pool": self._processing_pool_name,
                "pod_name": self._pod_name,
            }

            while True:
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
                needed = 0

                while True:
                    try:
                        message = messages.get(block=False)
                        received.append(message)
                    except queue.Empty:
                        break

                with self._children_lock:
                    children = list(self._children.items())

                    for cid, c in children:
                        if c.process.is_alive():
                            continue

                        c.process.join(timeout=0)
                        self._children.pop(cid)

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

                        if message.event == "ready":
                            if child.state != "pending":
                                logger.warning(
                                    "taskworker.child.ready_not_pending",
                                    extra={
                                        "cid": str(message.child_id),
                                        "state": child.state,
                                        "processing_pool": self._processing_pool_name,
                                    },
                                )

                            child.state = "ready"

                        elif message.event == "exiting":
                            if child.state != "ready":
                                logger.warning(
                                    "taskworker.child.exiting_not_ready",
                                    extra={
                                        "cid": str(message.child_id),
                                        "state": child.state,
                                        "processing_pool": self._processing_pool_name,
                                    },
                                )

                            child.state = "exiting"
                            needed += 1

                    pending = sum(1 for c in self._children.values() if c.state == "pending")
                    ready = sum(1 for c in self._children.values() if c.state == "ready")
                    exiting = sum(1 for c in self._children.values() if c.state == "exiting")

                    desired = self._concurrency
                    active = ready + exiting

                    if active > desired:
                        # We have too many exiting children, we can release some of them
                        releasable = active - desired
                        released = 0

                        for child in self._children.values():
                            if released == releasable:
                                break

                            if child.state == "exiting":
                                child.release.set()
                                released += 1
                    else:
                        # We may not have enough active children
                        missing = desired - active

                        # If we have N pending children and M needed, then we must spawn another M - N
                        needed += max(missing - pending, 0)

                for _ in range(needed):
                    child_id = uuid4()
                    release = self._mp_context.Event()

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
                            messages,
                            release,
                        ),
                    )

                    try:
                        process.start()
                    except Exception as e:
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

                    with self._children_lock:
                        self._children[child_id] = TrackedChild(
                            process=process,
                            state="pending",
                            release=release,
                        )

                    logger.info(
                        "taskworker.child.spawn",
                        extra={
                            "pid": process.pid,
                            "cid": str(child_id),
                            "processing_pool": self._processing_pool_name,
                        },
                    )

                    self._metrics.incr(
                        "taskworker.worker.child.spawn",
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
