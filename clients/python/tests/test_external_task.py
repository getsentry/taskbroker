from __future__ import annotations

from concurrent.futures import Future
from unittest.mock import Mock, patch

import pytest

from taskbroker_client.metrics import NoOpMetricsBackend
from taskbroker_client.registry import ExternalNamespace, TaskRegistry
from taskbroker_client.retry import Retry
from taskbroker_client.router import DefaultRouter

from .conftest import producer_factory


def make_external_namespace(
    name: str = "process",
    application: str = "launchpad",
) -> ExternalNamespace:
    return ExternalNamespace(
        name=name,
        application=application,
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
        retry=None,
    )


def test_external_task_raises_on_direct_call() -> None:
    ns = make_external_namespace()

    @ns.register(name="do_something")
    def do_something(x: int) -> None:
        pass

    with pytest.raises(ValueError, match="External tasks cannot be called locally"):
        do_something(1)


def test_external_task_always_eager() -> None:
    ns = make_external_namespace()

    @ns.register(name="do_something")
    def do_something(x: int) -> None:
        pass

    with patch("taskbroker_client.task.ALWAYS_EAGER", True):
        with pytest.raises(ValueError, match="External tasks cannot be called locally"):
            do_something(1)

        with pytest.raises(
            ValueError, match="External tasks cannot be called within an ALWAYS_EAGER block"
        ):
            do_something.delay(1)


def test_external_task_signature_type_preserved() -> None:
    ns = make_external_namespace()

    @ns.register(name="run_types")
    def run_types(*, name: str, cost: float, times: int) -> None:
        pass

    mock_producer = Mock()
    future: Future[None] = Future()
    future.set_result(None)
    mock_producer.produce.return_value = future
    ns._producers[ns.topic] = mock_producer

    # This shouldn't trigger any type errors.
    run_types.delay(name="bob", cost=2.3, times=1)

    assert mock_producer.produce.call_count == 1


def test_external_task_delay_dispatches_to_kafka() -> None:
    ns = make_external_namespace()

    @ns.register(name="do_something")
    def do_something(x: int) -> None:
        pass

    mock_producer = Mock()
    future: Future[None] = Future()
    future.set_result(None)
    mock_producer.produce.return_value = future
    ns._producers[ns.topic] = mock_producer

    do_something.delay(42)

    assert mock_producer.produce.call_count == 1
    call_args = mock_producer.produce.call_args
    assert call_args[0][0].name == ns.topic


def test_external_task_apply_async_dispatches_to_kafka() -> None:
    ns = make_external_namespace()

    @ns.register(name="do_something")
    def do_something(x: int) -> None:
        pass

    mock_producer = Mock()
    future: Future[None] = Future()
    future.set_result(None)
    mock_producer.produce.return_value = future
    ns._producers[ns.topic] = mock_producer

    do_something.apply_async(args=[42])

    assert mock_producer.produce.call_count == 1
    call_args = mock_producer.produce.call_args
    assert call_args[0][0].name == ns.topic


def test_external_namespace_stores_target_application_and_name() -> None:
    ns = make_external_namespace(name="process", application="launchpad")

    assert ns.name == "process"
    assert ns.application == "launchpad"


def test_external_namespace_topic_uses_prefixed_routing() -> None:
    class CapturingRouter:
        def __init__(self) -> None:
            self.last_name: str | None = None

        def route_namespace(self, name: str) -> str:
            if name == "launchpad:process":
                return "taskworker-launchpad-process"
            raise ValueError("Unkownn namespace name")

    router = CapturingRouter()
    ns = ExternalNamespace(
        name="process",
        application="launchpad",
        producer_factory=producer_factory,
        router=router,
        metrics=NoOpMetricsBackend(),
        retry=None,
    )
    assert ns.topic == "taskworker-launchpad-process"


def test_external_namespace_inherits_defaults() -> None:
    retry = Retry(times=3)
    ns = ExternalNamespace(
        name="process",
        application="launchpad",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
        retry=retry,
        expires=300,
        processing_deadline_duration=60,
    )

    @ns.register(name="my_task")
    def my_task() -> None:
        pass

    activation = my_task.create_activation([], {})
    assert activation.expires == 300
    assert activation.processing_deadline_duration == 60
    assert my_task.retry == retry


def test_external_namespace_no_retry_for_at_most_once() -> None:
    retry = Retry(times=3)
    ns = ExternalNamespace(
        name="process",
        application="launchpad",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
        retry=retry,
        expires=300,
        processing_deadline_duration=60,
    )

    @ns.register(name="my_task", at_most_once=True)
    def my_task() -> None:
        pass

    activation = my_task.create_activation([], {})
    assert activation.expires == 300
    assert activation.processing_deadline_duration == 60
    assert my_task.retry is None, "at_most_once tasks do not have retries"
    assert my_task.at_most_once is True


def test_external_namespace_register_overrides_defaults() -> None:
    override_retry = Retry(times=1)
    ns = ExternalNamespace(
        name="process",
        application="launchpad",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
        retry=Retry(times=5),
        expires=300,
        processing_deadline_duration=60,
    )

    @ns.register(
        name="my_task",
        retry=override_retry,
        expires=60,
        processing_deadline_duration=10,
    )
    def my_task() -> None:
        pass

    activation = my_task.create_activation([], {})
    assert activation.expires == 60
    assert activation.processing_deadline_duration == 10
    assert my_task.retry == override_retry


def test_registry_get_does_not_return_external_namespaces() -> None:
    registry = TaskRegistry(
        application="acme",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
    )
    registry.create_external_namespace(name="process", application="launchpad")

    with pytest.raises(KeyError):
        registry.get("process")

    with pytest.raises(KeyError):
        registry.get_task("process", "any_task")


def test_registry_get_external_returns_external_namespace() -> None:
    registry = TaskRegistry(
        application="acme",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
    )
    ns = registry.create_external_namespace(name="process", application="launchpad")

    assert registry.get_external("launchpad", "process") is ns


def test_registry_get_external_raises_for_unknown() -> None:
    registry = TaskRegistry(
        application="acme",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
    )

    with pytest.raises(KeyError, match="No external task namespace"):
        registry.get_external("nope", "nope")


def test_registry_create_external_namespace_duplicate_raises() -> None:
    registry = TaskRegistry(
        application="acme",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
    )
    registry.create_external_namespace(name="process", application="launchpad")

    with pytest.raises(ValueError, match="already exists"):
        registry.create_external_namespace(name="process", application="launchpad")


def test_task_activation_targets_external_application() -> None:
    """TaskActivation.application and .namespace point at the target app."""
    ns = make_external_namespace(name="process", application="launchpad")

    @ns.register(name="do_work")
    def do_work(x: int) -> None:
        pass

    activation = do_work.create_activation([1], {})
    assert activation.application == "launchpad"
    assert activation.namespace == "process"
    assert activation.taskname == "do_work"
