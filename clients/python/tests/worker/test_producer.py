from collections.abc import Iterator
from concurrent.futures import Future
from datetime import datetime

import pytest
from arroyo.backends.abstract import ProducerFuture, SimpleProducerFuture
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Partition, Topic

from taskbroker_client.worker.producer import TaskProducer, _pending_futures


def make_kafka_payload() -> KafkaPayload:
    """Generates dummy KafkaPayload."""
    return KafkaPayload(None, b"", [])


def make_broker_value() -> BrokerValue[KafkaPayload]:
    """Generates dummy BrokerValue[KafkaPayload]."""
    return BrokerValue(make_kafka_payload(), Partition(Topic("test"), 0), 0, datetime(1999, 2, 19))


class DummyProducer:
    def __init__(self, use_simple_futures: bool):
        self.use_simple_futures = use_simple_futures

    def produce(
        self, topic: Topic, payload: KafkaPayload
    ) -> ProducerFuture[BrokerValue[KafkaPayload]]:
        future: ProducerFuture[BrokerValue[KafkaPayload]]
        if self.use_simple_futures:
            future = SimpleProducerFuture()
        else:
            future = Future()
        future.set_result(make_broker_value())
        return future


@pytest.fixture(autouse=True)
def clear_pending_futures() -> Iterator[None]:
    _pending_futures.clear()
    yield
    _pending_futures.clear()


def test_producer_tracks_futures() -> None:
    producer = TaskProducer(DummyProducer(use_simple_futures=True))
    producer.produce(Topic("test"), make_kafka_payload())
    assert len(_pending_futures) == 1
    future = next(iter(TaskProducer.collect_futures()))
    assert future.result() == make_broker_value()
    assert len(_pending_futures) == 0


def test_producer_executes_callbacks() -> None:
    producer = TaskProducer(DummyProducer(use_simple_futures=False))
    received: list[Future[BrokerValue[KafkaPayload]]] = []

    def callback(future: Future[BrokerValue[KafkaPayload]]) -> None:
        received.append(future)

    producer.produce(Topic("test"), make_kafka_payload(), callbacks=[callback])
    tracked_future = next(iter(TaskProducer.collect_futures()))

    assert len(received) == 1
    assert received[0] is tracked_future
    assert received[0].done()


def test_producer_rejects_callbacks_for_simple_futures() -> None:
    producer = TaskProducer(DummyProducer(use_simple_futures=True))

    def callback(future: Future[BrokerValue[KafkaPayload]]) -> None:
        pass

    with pytest.raises(RuntimeError, match="SimpleProducerFuture"):
        producer.produce(Topic("test"), make_kafka_payload(), callbacks=[callback])
