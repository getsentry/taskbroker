"""
Minimal worker child: parse TaskActivation and return COMPLETE (proto demo).
Not a drop-in for full taskbroker child execution.
"""

from __future__ import annotations

import logging
import queue
import time
from multiprocessing.synchronize import Event

import orjson
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    TASK_ACTIVATION_STATUS_COMPLETE,
    TASK_ACTIVATION_STATUS_FAILURE,
)

from kafka_queue_worker.imports import import_string
from kafka_queue_worker.logging_config import configure_kqueue_logging
from kafka_queue_worker.types import InflightTaskActivation, NoOpMetrics, ProcessingResult, TaskActivation

logger = logging.getLogger(__name__)


def _params(activation: TaskActivation) -> object:
    if activation.parameters_bytes:
        try:
            return orjson.loads(activation.parameters_bytes)
        except orjson.JSONDecodeError:
            return {}
    if activation.parameters:
        if isinstance(activation.parameters, str):
            try:
                return orjson.loads(activation.parameters)
            except orjson.JSONDecodeError:
                return {}
    return {}


def child_process(
    app_module: str,
    child_tasks: queue.Queue[InflightTaskActivation],
    processed_tasks: queue.Queue[ProcessingResult],
    shutdown_event: Event,
    _max_task_count: int | None,
    processing_pool_name: str,
    _process_type: str,
) -> None:
    # Spawned children do not run the CLI; configure logging before any `logger` output.
    configure_kqueue_logging()
    # Resolve app (only used for .metrics; prototype ignores task registry)
    _app: object = import_string(app_module) if app_module else object()
    metrics: NoOpMetrics = (
        getattr(_app, "metrics", None) or NoOpMetrics()  # type: ignore[assignment]
    )
    if hasattr(_app, "load_modules"):
        getattr(_app, "load_modules")()

    while not shutdown_event.is_set():
        try:
            inflight = child_tasks.get(timeout=1.0)
        except queue.Empty:
            continue

        act = inflight.activation
        try:
            p = _params(act)
            logger.info(
                "kqueue execute task",
                extra={
                    "task": act.taskname,
                    "id": act.id,
                    "pool": processing_pool_name,
                    "params": p,
                },
            )
            st = int(TASK_ACTIVATION_STATUS_COMPLETE)
        except Exception as e:
            logger.exception("kqueue child failed: %s", e)
            st = int(TASK_ACTIVATION_STATUS_FAILURE)
        t = time.monotonic()
        _ = t
        processed_tasks.put(
            ProcessingResult(
                task_id=act.id,
                status=st,
                host=inflight.host,
                receive_timestamp=inflight.receive_timestamp,
                delivery_id=inflight.delivery_id,
            )
        )
        _ = metrics
