from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation

from examples.store import StubAtMostOnce
from taskbroker_client.app import TaskbrokerApp
from taskbroker_client.router import TaskRouter

from .conftest import producer_factory


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
