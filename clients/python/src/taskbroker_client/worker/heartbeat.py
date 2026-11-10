from __future__ import annotations

import logging
import multiprocessing
import queue
import signal
import threading
from collections import defaultdict
from collections.abc import Callable
from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
from multiprocessing.process import BaseProcess
from multiprocessing.synchronize import Event
from typing import Literal

import grpc

from taskbroker_client.app import import_app
from taskbroker_client.metrics import MetricsBackend
from taskbroker_client.types import InflightTaskActivation
from taskbroker_client.worker.client import TaskbrokerClient
from taskbroker_client.worker.push_clients import PushTaskbrokerClient

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL_SEC = 1.0

# ("track", {id: host}) or ("untrack", {id: ""})
HeartbeatCommand = tuple[Literal["track", "untrack"], dict[str, str]]


def heartbeat_process(
    app_module: str,
    broker_service: str | None,
    broker_hosts: list[str] | None,
    processing_pool_name: str,
    tracked: dict[str, str],
    commands: queue.Queue[HeartbeatCommand],
    shutdown_event: Event,
) -> None:
    """
    Renew the heartbeat of every tracked activation once per interval.

    `tracked` maps an activation id to the broker host it came from. Push mode sends
    every id to `broker_service`; pull mode sends each id to its own host.
    """
    # The parent owns shutdown and tells us through shutdown_event.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    app = import_app(app_module)
    metrics = app.metrics
    tags = {"processing_pool": processing_pool_name}

    send: Callable[[str, list[str]], list[str]]
    if broker_service is not None:
        push_client = PushTaskbrokerClient(
            service=broker_service,
            application=app.name,
            metrics=metrics,
            rpc_secret=app.config["rpc_secret"],
            grpc_config=app.config["grpc_config"],
            processing_pool_name=processing_pool_name,
        )
        send = lambda host, ids: push_client.update_heartbeats(ids)  # noqa: E731
    else:
        assert broker_hosts, "Pull mode heartbeats need broker_hosts"
        pull_client = TaskbrokerClient(
            hosts=broker_hosts,
            application=app.name,
            metrics=metrics,
            rpc_secret=app.config["rpc_secret"],
            grpc_config=app.config["grpc_config"],
            processing_pool_name=processing_pool_name,
        )
        send = pull_client.update_heartbeats

    tracked = dict(tracked)
    while not shutdown_event.is_set():
        while True:
            try:
                action, command_ids = commands.get_nowait()
            except queue.Empty:
                break
            if action == "track":
                tracked.update(command_ids)
            else:
                for task_id in command_ids:
                    tracked.pop(task_id, None)

        metrics.gauge("taskworker.worker.heartbeat.tracked", len(tracked), tags=tags)

        by_host: defaultdict[str, list[str]] = defaultdict(list)
        for task_id, host in tracked.items():
            by_host["" if broker_service is not None else host].append(task_id)

        for host, ids in by_host.items():
            try:
                not_found = send(host, ids)
            except grpc.RpcError as e:
                logger.warning(
                    "taskworker.worker.heartbeat.failed", extra={"error": e, "host": host, **tags}
                )
                continue

            for task_id in not_found:
                tracked.pop(task_id, None)
            if not_found:
                metrics.incr(
                    "taskworker.worker.heartbeat.not_found", value=len(not_found), tags=tags
                )

        shutdown_event.wait(HEARTBEAT_INTERVAL_SEC)


class HeartbeatTracker:
    """
    Runs `heartbeat_process` and restarts it if it dies.

    The parent keeps the tracked ids so a restarted process starts from them.
    Pass `broker_service` for push mode or `broker_hosts` for pull mode.
    """

    def __init__(
        self,
        app_module: str,
        processing_pool_name: str,
        mp_context: ForkContext | SpawnContext | ForkServerContext,
        metrics: MetricsBackend,
        broker_service: str | None = None,
        broker_hosts: list[str] | None = None,
    ) -> None:
        self._metrics = metrics
        self._app_module = app_module
        self._broker_service = broker_service
        self._broker_hosts = broker_hosts
        self._processing_pool_name = processing_pool_name
        self._mp_context = mp_context
        self._shutdown_event = mp_context.Event()
        self._lock = threading.Lock()
        self._tracked: dict[str, str] = {}
        self._commands: multiprocessing.Queue[HeartbeatCommand] | None = None
        self._process: BaseProcess | None = None
        self._supervisor: threading.Thread | None = None

    def track(self, inflight: InflightTaskActivation) -> None:
        """Track `inflight` if its task has a heartbeat."""
        if not inflight.activation.HasField("heartbeat"):
            return
        with self._lock:
            self._tracked[inflight.activation.id] = inflight.host
            if self._commands is not None:
                self._commands.put(("track", {inflight.activation.id: inflight.host}))

    def untrack(self, task_ids: list[str]) -> None:
        with self._lock:
            removed = {task_id: "" for task_id in task_ids if task_id in self._tracked}
            if not removed:
                return
            for task_id in removed:
                del self._tracked[task_id]
            if self._commands is not None:
                self._commands.put(("untrack", removed))

    def _spawn(self) -> None:
        with self._lock:
            # A fresh queue, so commands stranded in a dead process's queue are dropped
            # and the new process starts from the parent's ids instead.
            self._commands = self._mp_context.Queue()
            self._process = self._mp_context.Process(
                name="taskworker-heartbeat",
                target=heartbeat_process,
                args=(
                    self._app_module,
                    self._broker_service,
                    self._broker_hosts,
                    self._processing_pool_name,
                    dict(self._tracked),
                    self._commands,
                    self._shutdown_event,
                ),
                daemon=True,
            )
            self._process.start()

    def _supervise(self) -> None:
        while not self._shutdown_event.wait(HEARTBEAT_INTERVAL_SEC):
            if self._process is not None and not self._process.is_alive():
                self._metrics.incr(
                    "taskworker.worker.heartbeat.restarted",
                    tags={"processing_pool": self._processing_pool_name},
                )
                logger.warning(
                    "taskworker.worker.heartbeat.restarted",
                    extra={"processing_pool": self._processing_pool_name},
                )
                self._spawn()

    def start(self) -> None:
        # Start now, so the app import is done before the first task needs a heartbeat.
        self._spawn()
        self._supervisor = threading.Thread(
            name="heartbeat-supervisor", target=self._supervise, daemon=True
        )
        self._supervisor.start()

    def shutdown(self) -> None:
        self._shutdown_event.set()
        if self._process is not None:
            self._process.join(timeout=HEARTBEAT_INTERVAL_SEC * 2)
            if self._process.is_alive():
                self._process.terminate()
