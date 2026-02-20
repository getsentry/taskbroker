import pytest
from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation

from taskbroker_client.app import TaskbrokerApp
from taskbroker_client.retry import Retry
from taskbroker_client.router import TaskRouter
from taskbroker_client.task import Task

from .conftest import StubAtMostOnce, producer_factory


class StubRouter(TaskRouter):
    def route_namespace(self, name: str) -> str:
        return "honk"


def test_taskregistry_router_object() -> None:
    app = TaskbrokerApp(name="acme", producer_factory=producer_factory, router_class=StubRouter())
    ns = app.taskregistry.create_namespace("test")
    assert ns.topic == "honk"


def test_taskregistry_router_str() -> None:
    app = TaskbrokerApp(
        name="acme",
        producer_factory=producer_factory,
        router_class="taskbroker_client.router.DefaultRouter",
    )
    ns = app.taskregistry.create_namespace("test")
    assert ns.topic == "taskbroker"


def test_set_config() -> None:
    app = TaskbrokerApp(name="acme", producer_factory=producer_factory)
    app.set_config({"rpc_secret": "testing", "ignored": "key"})
    assert app.config["rpc_secret"] == "testing"
    assert "ignored" not in app.config


def test_should_attempt_at_most_once() -> None:
    activation = TaskActivation(
        id="111",
        taskname="examples.simple_task",
        namespace="examples",
        parameters='{"args": [], "kwargs": {}}',
        processing_deadline_duration=2,
    )
    at_most = StubAtMostOnce()
    app = TaskbrokerApp(name="acme", producer_factory=producer_factory)
    app.at_most_once_store(at_most)
    assert app.should_attempt_at_most_once(activation)
    assert not app.should_attempt_at_most_once(activation)


def test_create_namespace() -> None:
    app = TaskbrokerApp(name="acme", producer_factory=producer_factory, router_class=StubRouter())
    ns = app.create_namespace("test")
    assert ns.name == "test"
    assert ns.topic == "honk"

    retry = Retry(times=3)
    ns = app.create_namespace(
        "test-two",
        retry=retry,
        expires=60 * 10,
        processing_deadline_duration=60,
        app_feature="anvils",
    )
    assert ns.default_retry == retry
    assert ns.default_processing_deadline_duration == 60
    assert ns.default_expires == 60 * 10
    assert ns.name == "test-two"
    assert ns.application == "acme"
    assert ns.topic == "honk"
    assert ns.app_feature == "anvils"

    fetched = app.get_namespace("test-two")
    assert fetched == ns

    with pytest.raises(KeyError):
        app.get_namespace("invalid")


def test_get_task() -> None:
    app = TaskbrokerApp(name="acme", producer_factory=producer_factory, router_class=StubRouter())
    ns = app.create_namespace(name="tests")

    @ns.register(name="test.simpletask")
    def simple_task() -> None:
        raise NotImplementedError

    task = app.get_task(ns.name, "test.simpletask")
    assert isinstance(task, Task)

    with pytest.raises(KeyError):
        app.get_task("nope", "test.simpletask")

    with pytest.raises(KeyError):
        app.get_task(ns.name, "nope")
