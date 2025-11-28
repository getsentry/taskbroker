from datetime import UTC, datetime
from arroyo.backends.kafka import KafkaProducer

import time_machine

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
