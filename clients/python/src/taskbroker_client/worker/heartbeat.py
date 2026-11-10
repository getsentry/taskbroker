from __future__ import annotations

import logging
import multiprocessing
import queue
import signal
import threading
from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
from multiprocessing.process import BaseProcess
from multiprocessing.synchronize import Event
from typing import Literal

import grpc

from taskbroker_client.app import import_app
from taskbroker_client.metrics import MetricsBackend
from taskbroker_client.worker.push_clients import PushTaskbrokerClient

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL_SEC = 1.0

HeartbeatCommand = tuple[Literal["track", "untrack"], list[str]]


def heartbeat_process(
    app_module: str,
    broker_service: str,
    processing_pool_name: str,
    ids: list[str],
    commands: queue.Queue[HeartbeatCommand],
    shutdown_event: Event,
) -> None:
    """Renew the heartbeat of every tracked activation once per interval."""
    # The parent owns shutdown and tells us through shutdown_event.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    app = import_app(app_module)
    metrics = app.metrics
    client = PushTaskbrokerClient(
        service=broker_service,
        application=app.name,
        metrics=metrics,
        rpc_secret=app.config["rpc_secret"],
        grpc_config=app.config["grpc_config"],
        processing_pool_name=processing_pool_name,
    )
    tags = {"processing_pool": processing_pool_name}

    tracked = set(ids)
    while not shutdown_event.wait(HEARTBEAT_INTERVAL_SEC):
        while True:
            try:
                action, command_ids = commands.get_nowait()
            except queue.Empty:
                break
            if action == "track":
                tracked.update(command_ids)
            else:
                tracked.difference_update(command_ids)

        metrics.gauge("taskworker.worker.heartbeat.tracked", len(tracked), tags=tags)
        if not tracked:
            continue

        try:
            not_found = client.update_heartbeats(list(tracked))
        except grpc.RpcError as e:
            logger.warning("taskworker.worker.heartbeat.failed", extra={"error": e, **tags})
            continue

        if not_found:
            tracked.difference_update(not_found)
            metrics.incr("taskworker.worker.heartbeat.not_found", value=len(not_found), tags=tags)


class HeartbeatTracker:
    """
    Runs `heartbeat_process` once there is something to track, and restarts it if it dies.

    The parent keeps the set of tracked ids so a restarted process starts from it.
    """

    def __init__(
        self,
        app_module: str,
        broker_service: str,
        processing_pool_name: str,
        mp_context: ForkContext | SpawnContext | ForkServerContext,
        metrics: MetricsBackend,
    ) -> None:
        self._metrics = metrics
        self._app_module = app_module
        self._broker_service = broker_service
        self._processing_pool_name = processing_pool_name
        self._mp_context = mp_context
        self._shutdown_event = mp_context.Event()
        self._lock = threading.Lock()
        self._ids: set[str] = set()
        self._commands: multiprocessing.Queue[HeartbeatCommand] | None = None
        self._process: BaseProcess | None = None
        self._supervisor: threading.Thread | None = None

    def track(self, task_id: str) -> None:
        with self._lock:
            self._ids.add(task_id)
            if self._commands is not None:
                self._commands.put(("track", [task_id]))

    def untrack(self, task_ids: list[str]) -> None:
        with self._lock:
            removed = [task_id for task_id in task_ids if task_id in self._ids]
            if not removed:
                return
            self._ids.difference_update(removed)
            if self._commands is not None:
                self._commands.put(("untrack", removed))

    def _spawn(self) -> None:
        with self._lock:
            # A fresh queue, so commands stranded in a dead process's queue are dropped
            # and the new process starts from the parent's set instead.
            self._commands = self._mp_context.Queue()
            self._process = self._mp_context.Process(
                name="taskworker-heartbeat",
                target=heartbeat_process,
                args=(
                    self._app_module,
                    self._broker_service,
                    self._processing_pool_name,
                    list(self._ids),
                    self._commands,
                    self._shutdown_event,
                ),
                daemon=True,
            )
            self._process.start()

    def _supervise(self) -> None:
        while not self._shutdown_event.wait(HEARTBEAT_INTERVAL_SEC):
            # Start lazily, so workers without heartbeat tasks don't pay for the process.
            if self._process is None:
                if self._ids:
                    self._spawn()
            elif not self._process.is_alive():
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
