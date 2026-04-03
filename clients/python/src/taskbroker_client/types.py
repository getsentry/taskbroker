import contextlib
import dataclasses
from typing import Callable, Protocol

from arroyo.backends.abstract import ProducerFuture
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Topic
from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation, TaskActivationStatus


class ContextHook(Protocol):
    """
    Hook for propagating application context through task headers.

    on_dispatch: called at task creation time to inject context into headers.
    on_execute: called at task execution time, returns a context manager
                that restores context from headers for the duration of the task.
    """

    def on_dispatch(self, headers: dict[str, str]) -> None: ...

    def on_execute(self, headers: dict[str, str]) -> contextlib.AbstractContextManager[None]: ...


class AtMostOnceStore(Protocol):
    """
    Interface for the at_most_once store used for idempotent task execution.
    """

    def add(self, key: str, value: str, timeout: int) -> bool: ...


class ProducerProtocol(Protocol):
    """Interface for producers that tasks depend on."""

    def produce(
        self, topic: Topic, payload: KafkaPayload
    ) -> ProducerFuture[BrokerValue[KafkaPayload]]: ...


ProducerFactory = Callable[[str], ProducerProtocol]
"""
A factory interface for resolving topics into a KafkaProducer
that can produce on the provided topic.
"""


@dataclasses.dataclass
class InflightTaskActivation:
    """
    A TaskActivation with Metadata used within workers.
    """

    activation: TaskActivation
    host: str
    receive_timestamp: float


@dataclasses.dataclass
class ProcessingResult:
    """Result structure from child processess to parent"""

    task_id: str
    status: TaskActivationStatus.ValueType
    host: str
    receive_timestamp: float
