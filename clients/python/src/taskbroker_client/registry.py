from __future__ import annotations

import datetime
import logging
from collections.abc import Callable
from concurrent import futures
from typing import Any

import sentry_sdk
from arroyo.backends.kafka import KafkaPayload, KafkaProducer
from arroyo.types import BrokerValue, Topic
from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation
from sentry_sdk.consts import OP, SPANDATA

from taskbroker_client.constants import DEFAULT_PROCESSING_DEADLINE, CompressionType

from taskbroker_client.metrics import MetricsBackend
from taskbroker_client.retry import Retry
from taskbroker_client.router import TaskRouter
from taskbroker_client.task import P, R, Task
from taskbroker_client.types import ProducerFactory

logger = logging.getLogger(__name__)

ProducerFuture = futures.Future[BrokerValue[KafkaPayload]]


class TaskNamespace:
    """
    Task namespaces link topics, config and default retry mechanics together
    All tasks within a namespace are stored in the same topic and run by shared
    worker pool.
    """

    def __init__(
        self,
        name: str,
        producer_factory: ProducerFactory,
        router: TaskRouter,
        metrics: MetricsBackend,
        retry: Retry | None,
        expires: int | datetime.timedelta | None = None,
        processing_deadline_duration: int = DEFAULT_PROCESSING_DEADLINE,
        app_feature: str | None = None,
    ):
        self.name = name
        self.router = router
        self.default_retry = retry
        self.default_expires = expires  # seconds
        self.default_processing_deadline_duration = processing_deadline_duration  # seconds
        self.app_feature = app_feature or name
        self._registered_tasks: dict[str, Task[Any, Any]] = {}
        self._producers: dict[str, KafkaProducer] = {}
        self._producer_factory = producer_factory
        self.metrics = metrics

    def get(self, name: str) -> Task[Any, Any]:
        """
        Get a registered task by name

        Raises KeyError when an unknown task is provided.
        """
        if name not in self._registered_tasks:
            raise KeyError(f"No task registered with the name {name}. Check your imports")
        return self._registered_tasks[name]

    def contains(self, name: str) -> bool:
        """
        Check if a task name has been registered
        """
        return name in self._registered_tasks

    @property
    def topic(self) -> str:
        return self.router.route_namespace(self.name)

    def register(
        self,
        *,
        name: str,
        retry: Retry | None = None,
        expires: int | datetime.timedelta | None = None,
        processing_deadline_duration: int | datetime.timedelta | None = None,
        at_most_once: bool = False,
        wait_for_delivery: bool = False,
        compression_type: CompressionType = CompressionType.PLAINTEXT,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        """
        Register a task.

        Applied as a decorator to functions to enable them to be run
        asynchronously via taskworkers.

        Parameters
        ----------

        name: str
            The name of the task. This is serialized and must be stable across deploys.
        retry: Retry | None
            The retry policy for the task. If none and at_most_once is not enabled
            the Task namespace default retry policy will be used.
        expires: int | datetime.timedelta
            The number of seconds a task activation is valid for. After this
            duration the activation will be discarded and not executed.
        at_most_once : bool
            Enable at-most-once execution. Tasks with `at_most_once` cannot
            define retry policies, and use a worker side idempotency key to
            prevent processing deadline based retries.
        wait_for_delivery: bool
            If true, the task will wait for the delivery report to be received
            before returning.
        compression_type: CompressionType
            The compression type to use to compress the task parameters.
        """

        def wrapped(func: Callable[P, R]) -> Task[P, R]:
            task_retry = retry
            if not at_most_once:
                task_retry = retry or self.default_retry
            task = Task(
                name=name,
                func=func,
                namespace=self,
                retry=task_retry,
                expires=expires or self.default_expires,
                processing_deadline_duration=(
                    processing_deadline_duration or self.default_processing_deadline_duration
                ),
                at_most_once=at_most_once,
                wait_for_delivery=wait_for_delivery,
                compression_type=compression_type,
            )
            # TODO(taskworker) tasks should be registered into the registry
            # so that we can ensure task names are globally unique
            self._registered_tasks[name] = task
            return task

        return wrapped

    def _handle_produce_future(self, future: ProducerFuture, tags: dict[str, str]) -> None:
        if future.cancelled():
            self.metrics.incr("taskworker.registry.send_task.cancelled", tags=tags)
        elif future.exception(1):
            # this does not block since this callback only gets run when the future is finished and exception is set
            self.metrics.incr("taskworker.registry.send_task.failed", tags=tags)
        else:
            self.metrics.incr("taskworker.registry.send_task.success", tags=tags)

    def send_task(self, activation: TaskActivation, wait_for_delivery: bool = False) -> None:
        topic = self.router.route_namespace(self.name)

        with sentry_sdk.start_span(
            op=OP.QUEUE_PUBLISH,
            name=activation.taskname,
            origin="taskworker",
        ) as span:
            span.set_data(SPANDATA.MESSAGING_DESTINATION_NAME, activation.namespace)
            span.set_data(SPANDATA.MESSAGING_MESSAGE_ID, activation.id)
            span.set_data(SPANDATA.MESSAGING_SYSTEM, "taskworker")

            produce_future = self._producer(topic).produce(
                Topic(name=topic),
                KafkaPayload(key=None, value=activation.SerializeToString(), headers=[]),
            )

        self.metrics.incr(
            "taskworker.registry.send_task.scheduled",
            tags={
                "namespace": activation.namespace,
                "taskname": activation.taskname,
                "topic": topic,
            },
        )
        # We know this type is futures.Future, but cannot assert so,
        # because it is also mock.Mock in tests.
        produce_future.add_done_callback(  # type:ignore[union-attr]
            lambda future: self._handle_produce_future(
                future=future,
                tags={
                    "namespace": activation.namespace,
                    "taskname": activation.taskname,
                    "topic": topic,
                },
            )
        )
        if wait_for_delivery:
            try:
                produce_future.result(timeout=10)
            except Exception:
                logger.exception("Failed to wait for delivery")

    def _producer(self, topic: str) -> KafkaProducer:
        if topic not in self._producers:
            self._producers[topic] = self._producer_factory(topic)
        return self._producers[topic]


# TODO(mark) All of TaskRegistry could be folded into TaskworkerApp later.
class TaskRegistry:
    """
    Registry of all namespaces.

    The TaskRegistry is responsible for handling namespace -> topic resolution
    during startup.
    """

    def __init__(
        self,
        producer_factory: ProducerFactory,
        router: TaskRouter,
        metrics: MetricsBackend,
    ) -> None:
        self._namespaces: dict[str, TaskNamespace] = {}
        self._producer_factory = producer_factory
        self._router = router
        self._metrics = metrics

    def contains(self, name: str) -> bool:
        return name in self._namespaces

    def get(self, name: str) -> TaskNamespace:
        """Fetch a namespace by name."""
        if name not in self._namespaces:
            raise KeyError(f"No task namespace with the name {name}")
        return self._namespaces[name]

    def get_task(self, namespace: str, task: str) -> Task[Any, Any]:
        """Fetch a task by namespace and name."""
        return self.get(namespace).get(task)

    def create_namespace(
        self,
        name: str,
        *,
        retry: Retry | None = None,
        expires: int | datetime.timedelta | None = None,
        processing_deadline_duration: int = DEFAULT_PROCESSING_DEADLINE,
        app_feature: str | None = None,
    ) -> TaskNamespace:
        """
        Create a task namespace.

        Namespaces are mapped onto topics through the configured router allowing
        infrastructure to be scaled based on a region's requirements.

        Namespaces can define default behavior for tasks defined within a namespace.
        """
        if name in self._namespaces:
            raise ValueError(f"Task namespace with name {name} already exists.")
        namespace = TaskNamespace(
            name=name,
            router=self._router,
            metrics=self._metrics,
            producer_factory=self._producer_factory,
            retry=retry,
            expires=expires,
            processing_deadline_duration=processing_deadline_duration,
            app_feature=app_feature,
        )
        self._namespaces[name] = namespace

        return namespace
