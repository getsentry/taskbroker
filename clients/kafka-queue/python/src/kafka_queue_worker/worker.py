"""
Kafka queue prototype worker: copied shape from taskbroker ``TaskWorker``; uses
:class:`QueueBridgeClient` instead of ``TaskbrokerClient``.
"""

from __future__ import annotations

import logging
import multiprocessing
import queue
import signal
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
from multiprocessing.process import BaseProcess
from typing import Any, Optional, TYPE_CHECKING

import grpc
from sentry_protos.taskbroker.v1.taskbroker_pb2 import FetchNextTask

from kafka_queue_worker.child import child_process
from kafka_queue_worker.imports import import_string
from kafka_queue_worker.queue_bridge_client import QueueBridgeClient
from kafka_queue_worker.requeue import RequeueException
from kafka_queue_worker.types import InflightTaskActivation, NoOpMetrics, ProcessingResult

if TYPE_CHECKING:
    AppLike = object

logger = logging.getLogger(__name__)

_DEFAULT_QUEUE = 128
DEFAULT_REBALANCE_AFTER = 8


class TaskQueueWorker:
    """
    One worker: fetch from Java bridge via gRPC, execute in fork/spawn children.

    * ``bridge_url`` replaces taskbroker gRPC host list, e.g. ``queue-bridge:50060`` (same k8s namespace).
    * ``rebalance_after`` is ignored (kept for a familiar constructor).
    """

    def __init__(
        self,
        app_module: str,
        bridge_url: str,
        max_child_task_count: int | None = None,
        namespace: str | None = None,
        concurrency: int = 1,
        child_tasks_queue_maxsize: int = _DEFAULT_QUEUE,
        result_queue_maxsize: int = _DEFAULT_QUEUE,
        rebalance_after: int = DEFAULT_REBALANCE_AFTER,
        processing_pool_name: str | None = None,
        process_type: str = "spawn",
        health_check_file_path: str | None = None,
    ) -> None:
        _ = rebalance_after
        _ = health_check_file_path
        self._namespace: str | None = namespace
        self._mp_context: ForkContext | SpawnContext | ForkServerContext
        if process_type == "fork":
            self._mp_context = multiprocessing.get_context("fork")
        elif process_type == "spawn":
            self._mp_context = multiprocessing.get_context("spawn")
        elif process_type == "forkserver":
            self._mp_context = multiprocessing.get_context("forkserver")
        else:
            raise ValueError(process_type)

        _app: AppLike = import_string(app_module)
        metrics: NoOpMetrics = (
            getattr(_app, "metrics", None) or NoOpMetrics()  # type: ignore[assignment]
        )
        if hasattr(_app, "name"):
            app_name: str = str(getattr(_app, "name", "kqueue-proto"))
        else:
            app_name = "kqueue-proto"

        self._metrics = metrics
        self._processing_pool_name: str = processing_pool_name or "kqueue"
        self.worker_pool = _ProcessingPool(
            app_module=app_module,
            mp_context=self._mp_context,
            on_complete=self._on_complete,
            max_child_task_count=max_child_task_count,
            concurrency=concurrency,
            child_tasks_queue_maxsize=child_tasks_queue_maxsize,
            result_queue_maxsize=result_queue_maxsize,
            processing_pool_name=self._processing_pool_name,
            process_type=process_type,
        )
        self._grpc_sync = self._mp_context.Event()
        self._get_backoff = 0
        self._put_backoff = 0
        self.client = QueueBridgeClient(bridge_url, metrics, application=app_name)

    def _on_complete(
        self, result: ProcessingResult, skip_fetch_next: bool = False
    ) -> InflightTaskActivation | None:
        """``skip_fetch_next`` = draining shutdown or child queue is full (same as reference)."""
        self._metrics.distribution(
            "kqueue.worker.complete_duration",
            time.monotonic() - result.receive_timestamp,
            {"processing_pool": self._processing_pool_name},
        )
        fetch: FetchNextTask | None
        if skip_fetch_next:
            fetch = None
        else:
            fetch = FetchNextTask(application=self.client.application)
            if self._namespace is not None:
                fetch.namespace = self._namespace
        self._grpc_sync.wait(self._put_backoff)
        try:
            n = self.client.update_task(result, fetch)
            self._put_backoff = 0
            return n
        except grpc.RpcError as e:
            self._put_backoff = min(self._put_backoff + 1, 10)
            logger.warning("kqueue.set_status", extra={"task": result.task_id, "e": e})
            raise RequeueException(str(e))

    def fetch_task(self) -> InflightTaskActivation | None:
        self._grpc_sync.wait(self._get_backoff)
        try:
            t = self.client.get_task(self._namespace)
        except grpc.RpcError as e:
            self._get_backoff = min(self._get_backoff + 1, 10)
            logger.info("kqueue.get_task failed: %s", e)
            return None
        if t is None:
            self._get_backoff = min(self._get_backoff + 1, 2)
        else:
            self._get_backoff = 0
        return t

    def start(self) -> int:
        self.worker_pool.start_result_thread()
        self.worker_pool.start_spawn_children_thread()

        def h(*_a: object) -> None:
            raise KeyboardInterrupt()

        signal.signal(signal.SIGINT, h)
        signal.signal(signal.SIGTERM, h)
        try:
            while True:
                self._add_task()
        except KeyboardInterrupt:
            self.shutdown()
            return 0

    def _add_task(self) -> bool:
        if self.worker_pool.is_full():
            time.sleep(0.1)
            return False
        inflight = self.fetch_task()
        if not inflight:
            time.sleep(0.05)
            return False
        return self.worker_pool.push(inflight)

    def shutdown(self) -> None:
        self._grpc_sync.set()
        self.worker_pool.shutdown()
        self.client.close()


class _ProcessingPool:
    def __init__(
        self,
        app_module: str,
        on_complete: Callable[[ProcessingResult, bool], Optional[InflightTaskActivation]],
        mp_context: ForkContext | SpawnContext | ForkServerContext,
        max_child_task_count: int | None = None,
        concurrency: int = 1,
        child_tasks_queue_maxsize: int = _DEFAULT_QUEUE,
        result_queue_maxsize: int = _DEFAULT_QUEUE,
        processing_pool_name: str = "kqueue",
        process_type: str = "spawn",
    ) -> None:
        self._concurrency = concurrency
        self._name = processing_pool_name
        self._on_complete = on_complete
        self._app_module = app_module
        self._mp = mp_context
        self._process_type = process_type
        self._maxc = max_child_task_count
        self._shutdown = self._mp.Event()
        self._ctasks: multiprocessing.Queue[InflightTaskActivation] = self._mp.Queue(
            maxsize=child_tasks_queue_maxsize
        )
        self._results: multiprocessing.Queue[ProcessingResult] = self._mp.Queue(maxsize=result_queue_maxsize)
        self._children: list[BaseProcess] = []
        self._rthread: Optional[threading.Thread] = None
        self._sthread: Optional[threading.Thread] = None
        self._metrics: NoOpMetrics = NoOpMetrics()
        a = import_string(self._app_module) if self._app_module else None
        if a is not None and getattr(a, "metrics", None) is not None:
            self._metrics = a.metrics  # type: ignore[assignment]

    def _deliver_result(
        self, r: ProcessingResult, draining: bool
    ) -> InflightTaskActivation | None:
        try:
            skip = draining or self._ctasks.full()
            n = self._on_complete(r, skip)
            if n is not None:
                self._ctasks.put(n, timeout=30.0)
            return n
        except RequeueException:
            if not draining:
                self._results.put(r)
        return None

    def _result_thread(self) -> None:
        # One thread: gRPC client is not hardened for parallel Complete.
        pool = ThreadPoolExecutor(max_workers=1)
        with pool as ex:
            while not self._shutdown.is_set():
                try:
                    r = self._results.get(timeout=1.0)
                except queue.Empty:
                    continue
                ex.submit(self._deliver_result, r, False)

    def _spawn_thread(self) -> None:
        while not self._shutdown.is_set():
            self._children = [c for c in self._children if c.is_alive()]
            if len(self._children) >= self._concurrency:
                time.sleep(0.1)
                continue
            for _ in range(self._concurrency - len(self._children)):
                p = self._mp.Process(
                    name="kqueue-child",
                    target=child_process,
                    args=(
                        self._app_module,
                        self._ctasks,
                        self._results,
                        self._shutdown,
                        self._maxc,
                        self._name,
                        self._process_type,
                    ),
                )
                p.start()
                self._children.append(p)

    def start_result_thread(self) -> None:
        self._rthread = threading.Thread(target=self._result_thread, daemon=True)
        self._rthread.start()
        _ = self._rthread  # for linters

    def start_spawn_children_thread(self) -> None:
        self._sthread = threading.Thread(target=self._spawn_thread, daemon=True)
        self._sthread.start()

    def is_full(self) -> bool:
        return self._ctasks.full()

    def push(self, t: InflightTaskActivation) -> bool:
        try:
            self._ctasks.put(t, timeout=30.0)
            return True
        except queue.Full:
            return False

    def shutdown(self) -> None:
        self._shutdown.set()
        if self._sthread is not None:
            self._sthread.join()
        for c in self._children:
            c.terminate()
        for c in self._children:
            c.join()
        if self._rthread is not None:
            self._rthread.join(timeout=5.0)
        while True:
            try:
                r = self._results.get_nowait()
            except queue.Empty:
                break
            self._deliver_result(r, True)
