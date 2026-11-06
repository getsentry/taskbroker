import contextlib
import dataclasses
from collections.abc import MutableMapping
from concurrent.futures import Future
from typing import Any, Callable, Protocol

from arroyo.backends.abstract import ProducerFuture
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Partition, Topic
from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation, TaskActivationStatus

TaskHeaders = dict[str, str]
"""Headers passed to a task function when pass_headers=True is set."""


class ContextHook(Protocol):
    """
    Hook for propagating application context through task headers.

    on_dispatch: called at task creation time to inject context into headers.
    on_execute: called at task execution time, returns a context manager
                that restores context from headers for the duration of the task.
    """

    def on_dispatch(self, headers: MutableMapping[str, Any]) -> None: ...

    def on_execute(self, headers: dict[str, str]) -> contextlib.AbstractContextManager[None]: ...


class TaskCompletionHook(Protocol):
    """
    Hook called in the worker child after every activation, once the
    processing deadline alarm has been disarmed.

    The deadline is enforced by raising a BaseException from a signal
    handler, so it can interrupt library code at an arbitrary point and leave
    process-wide state (database connections, client sessions) broken for
    the next task on the same child. This hook is where an application can
    check that state and reset it before the next task runs.

    deadline_exceeded is True when the processing deadline fired during the
    task, including when another exception replaced it. Hooks should not rely
    on it alone: a deadline can be swallowed entirely by code that catches
    BaseException.
    """

    def after_task(
        self,
        activation: TaskActivation,
        status: TaskActivationStatus.ValueType,
        deadline_exceeded: bool,
    ) -> None: ...


class AtMostOnceStore(Protocol):
    """
    Interface for the at_most_once store used for idempotent task execution.
    """

    def add(self, key: str, value: str, timeout: int) -> bool: ...


class ProducerProtocol(Protocol):
    """Interface for producers that tasks depend on."""

    def produce(
        self, dest: Topic | Partition, payload: KafkaPayload
    ) -> ProducerFuture[BrokerValue[KafkaPayload]]: ...


class CloseableProducerProtocol(Protocol):
    """Interface used by TaskProducer. Represents a producer that has a shutdown method."""

    def produce(
        self, dest: Topic | Partition, payload: KafkaPayload
    ) -> ProducerFuture[BrokerValue[KafkaPayload]]: ...

    def close(self) -> Future[None]: ...


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
    max_attempts: int | None = None
    delay_on_retry: int | None = None
