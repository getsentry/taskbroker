"""
Example taskbroker application with tasks

Used in tests for the worker.
"""

import logging
from time import sleep
from typing import Any
from redis import StrictRedis
from arroyo.backends.kafka import KafkaProducer

from taskbroker_client.app import TaskbrokerApp
from taskbroker_client.retry import LastAction, NoRetriesRemainingError, Retry, RetryTaskError
from taskbroker_client.retry import retry_task as retry_task_helper
from examples.store import StubAtMostOnce

logger = logging.getLogger(__name__)

def producer_factory(topic: str) -> KafkaProducer:
    # TODO use env vars for kafka host/port
    config = {
        "bootstrap.servers": "127.0.0.1:9092",
        "compression.type": "lz4",
        "message.max.bytes": 50000000,  # 50MB
    }
    return KafkaProducer(config)


app = TaskbrokerApp(
    producer_factory=producer_factory,
    at_most_once_store=StubAtMostOnce(),
)

# Create a namespace and register tasks
exampletasks = app.taskregistry.create_namespace("examples")


@exampletasks.register(name="examples.simple_task")
def simple_task(*args: list[Any], **kwargs: dict[str, Any]) -> None:
    sleep(0.1)
    logger.debug("simple_task complete")


@exampletasks.register(name="examples.retry_task", retry=Retry(times=2))
def retry_task() -> None:
    raise RetryTaskError


@exampletasks.register(name="examples.fail_task")
def fail_task() -> None:
    raise ValueError("nope")


@exampletasks.register(name="examples.at_most_once", at_most_once=True)
def at_most_once_task() -> None:
    pass


@exampletasks.register(
    name="examples.retry_state", retry=Retry(times=2, times_exceeded=LastAction.Deadletter)
)
def retry_state() -> None:
    try:
        retry_task_helper()
    except NoRetriesRemainingError:
        # TODO read host from env vars
        redis = StrictRedis(host="localhost", port=6379, decode_responses=True)
        redis.set("no-retries-remaining", 1)


@exampletasks.register(
    name="examples.will_retry",
    retry=Retry(times=3, on=(RuntimeError,), times_exceeded=LastAction.Discard),
)
def will_retry(failure: str) -> None:
    if failure == "retry":
        logger.debug("going to retry with explicit retry error")
        raise RetryTaskError
    if failure == "raise":
        logger.debug("raising runtimeerror")
        raise RuntimeError("oh no")
    logger.debug("got %s", failure)


@exampletasks.register(name="examples.timed")
def timed_task(sleep_seconds: float | str, *args: list[Any], **kwargs: dict[str, Any]) -> None:
    sleep(float(sleep_seconds))
    logger.debug("timed_task complete")
