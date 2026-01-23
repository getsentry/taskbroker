from arroyo.backends.kafka import KafkaProducer

from examples.store import StubAtMostOnce
from taskbroker_client.app import TaskbrokerApp


def producer_factory(topic: str) -> KafkaProducer:
    # TODO use env vars for kafka host/port
    config = {
        "bootstrap.servers": "127.0.0.1:9092",
        "compression.type": "lz4",
        "message.max.bytes": 50000000,  # 50MB
    }
    return KafkaProducer(config)


app = TaskbrokerApp(
    name="example-app",
    producer_factory=producer_factory,
    at_most_once_store=StubAtMostOnce(),
)
app.set_modules(["examples.tasks"])
