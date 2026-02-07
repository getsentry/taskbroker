from datetime import UTC, datetime

import time_machine
from arroyo.backends.kafka import KafkaProducer

from taskbroker_client.types import AtMostOnceStore


def producer_factory(topic: str) -> KafkaProducer:
    config = {
        "bootstrap.servers": "127.0.0.1:9092",
        "compression.type": "lz4",
        "message.max.bytes": 50000000,  # 50MB
    }
    return KafkaProducer(config)


def freeze_time(t: str | datetime | None = None) -> time_machine.travel:
    if t is None:
        t = datetime.now(UTC)
    return time_machine.travel(t, tick=False)


class StubAtMostOnce(AtMostOnceStore):
    def __init__(self) -> None:
        self._keys: dict[str, str] = {}

    def add(self, key: str, value: str, timeout: int) -> bool:
        if key in self._keys:
            return False
        self._keys[key] = value
        return True
