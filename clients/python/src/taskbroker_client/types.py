from typing import Callable, Protocol

from arroyo.backends.kafka import KafkaProducer


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
