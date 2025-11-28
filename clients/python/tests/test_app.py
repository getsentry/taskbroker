from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation

from taskbroker_client.app import AtMostOnceStore, TaskworkerApp
from taskbroker_client.registry import TaskRegistry


class StubAtMostOnce(AtMostOnceStore):
    def __init__(self) -> None:
        self._keys: dict[str, str] = {}

    def add(self, key: str, value: str, timeout: int) -> bool:
        if key in self._keys:
            return False
        self._keys[key] = value
        return True


def test_taskregistry_param_and_property() -> None:
    registry = TaskRegistry()
    app = TaskworkerApp(taskregistry=registry)
    assert app.taskregistry == registry


def test_set_config() -> None:
    app = TaskworkerApp()
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
    app = TaskworkerApp()
    app.at_most_once_store(at_most)
    assert app.should_attempt_at_most_once(activation)
    assert not app.should_attempt_at_most_once(activation)
