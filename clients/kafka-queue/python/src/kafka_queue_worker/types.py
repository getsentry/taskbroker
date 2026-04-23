from __future__ import annotations

import dataclasses
from collections.abc import MutableMapping
from typing import Any

from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    TASK_ACTIVATION_STATUS_COMPLETE,
    TASK_ACTIVATION_STATUS_FAILURE,
    TASK_ACTIVATION_STATUS_RETRY,
    FetchNextTask,
    TaskActivation,
)

__all__ = [
    "BRIDGE_HOST_TAG",
    "FetchNextTask",
    "InflightTaskActivation",
    "NoOpMetrics",
    "ProcessingResult",
    "TASK_ACTIVATION_STATUS_COMPLETE",
    "TASK_ACTIVATION_STATUS_FAILURE",
    "TASK_ACTIVATION_STATUS_RETRY",
    "TaskActivation",
]

BRIDGE_HOST_TAG = "queue-bridge"


@dataclasses.dataclass
class InflightTaskActivation:
    activation: TaskActivation
    host: str
    receive_timestamp: float
    delivery_id: str


@dataclasses.dataclass
class ProcessingResult:
    task_id: str
    status: int
    host: str
    receive_timestamp: float
    delivery_id: str
    fetch_next_namespace: str | None = None


class NoOpMetrics:
    def incr(self, _key: str, _tags: MutableMapping[str, Any] | None = None) -> None:
        pass

    def distribution(self, _key: str, _value: float, _tags: MutableMapping[str, Any] | None = None) -> None:
        pass

    def timer(
        self, _key: str, _tags: MutableMapping[str, Any] | None = None
    ) -> _NoOpTimer:
        return _NoOpTimer()

    def gauge(self, _key: str, _value: float) -> None:
        pass


class _NoOpTimer:
    def __enter__(self) -> _NoOpTimer:
        return self

    def __exit__(self, *args: object) -> None:
        return None
