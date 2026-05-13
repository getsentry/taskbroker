from collections.abc import Callable
from concurrent.futures import Future
from typing import Any, Sequence

from arroyo.backends.abstract import ProducerFuture, SimpleProducerFuture
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Topic

from taskbroker_client.types import ProducerProtocol

# This is global as TaskWorker needs to be able to call TaskProducer.collect_futures()
# without a reference to a task's specific instance of TaskProducer.
_pending_futures: set[ProducerFuture[BrokerValue[KafkaPayload]]] = set()


class TaskProducer:
    """
    TaskProducer is a producer abstraction that should be used by tasks
    when producing to Kafka is a side effect of a task function.
    After a TaskWorker child process executes a task function, it will collect all
    producer futures tracked by TaskProducer, and will only register the activation as
    a success if all producer futures from that task were successful.
    Otherwise, the activation will be retried.

    TODO: actually have the TaskWorker child check TaskProducer futures.
    """

    def __init__(self, producer: ProducerProtocol) -> None:
        self._inner_producer = producer

    def track_future(self, future: ProducerFuture[BrokerValue[KafkaPayload]]) -> None:
        _pending_futures.add(future)

    @staticmethod
    def collect_futures() -> set[ProducerFuture[BrokerValue[KafkaPayload]]]:
        futures = _pending_futures.copy()
        _pending_futures.clear()
        return futures

    def produce(
        self,
        topic: Topic,
        payload: KafkaPayload,
        callbacks: Sequence[Callable[[Future[BrokerValue[KafkaPayload]]], Any]] = [],
    ) -> None:
        """
        Produces the given payload to the given topic.
        Since TaskProducer tracks futures internally, it does not return the
        producer future to the user, but the user can still add callbacks
        to the future via the `callbacks` arg.

        Args:
            topic: Topic to produce to.
            payload: KafkaPayload to produce.
            callbacks: List of Callables to add to the future as done callbacks. The future itself
                       is the only arg passed to the callback.
        """
        future = self._inner_producer.produce(topic, payload)
        self.track_future(future)
        if callbacks:
            # Arroyo producers can return a SimpleProducerFuture,
            # which does not accept callbacks.
            if not isinstance(future, SimpleProducerFuture):
                for c in callbacks:
                    future.add_done_callback(c)
            else:
                raise RuntimeError(
                    (
                        "Cannot add callbacks to SimpleProducerFuture, either remove the callbacks "
                        "or instantiate your producer with `use_simple_futures=False`."
                    )
                )
