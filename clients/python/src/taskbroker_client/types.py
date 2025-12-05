import dataclasses
from typing import Callable, Protocol

from arroyo.backends.kafka import KafkaProducer
from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation, TaskActivationStatus


class AtMostOnceStore(Protocol):
    """
    Interface for the at_most_once store used for idempotent task execution.
    """
    def add(self, key: str, value: str, timeout: int) -> bool: ...


ProducerFactory = Callable[[str], KafkaProducer]
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
