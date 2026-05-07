import contextlib
import datetime
from collections.abc import MutableMapping
from typing import Any
from unittest.mock import patch

import msgpack
import orjson
import pytest
import sentry_sdk
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    ON_ATTEMPTS_EXCEEDED_DEADLETTER,
    ON_ATTEMPTS_EXCEEDED_DISCARD,
)

from taskbroker_client.metrics import NoOpMetricsBackend
from taskbroker_client.registry import TaskNamespace
from taskbroker_client.retry import LastAction, Retry, RetryTaskError
from taskbroker_client.router import DefaultRouter
from taskbroker_client.task import Task

from .conftest import producer_factory


def do_things() -> None:
    raise NotImplementedError


@pytest.fixture
def task_namespace() -> TaskNamespace:
    return TaskNamespace(
        name="tests",
        application="acme",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
        retry=None,
    )


def test_define_task_defaults(task_namespace: TaskNamespace) -> None:
    task = Task(name="test.do_things", func=do_things, namespace=task_namespace)
    assert task.retry is None
    assert task.name == "test.do_things"
    assert task.namespace == task_namespace


def test_define_task_retry(task_namespace: TaskNamespace) -> None:
    retry = Retry(times=3, times_exceeded=LastAction.Deadletter)
    task = Task(name="test.do_things", func=do_things, namespace=task_namespace, retry=retry)
    assert task.retry == retry


def test_define_task_at_most_once_with_retry(task_namespace: TaskNamespace) -> None:
    with pytest.raises(AssertionError) as err:
        Task(
            name="test.do_things",
            func=do_things,
            namespace=task_namespace,
            at_most_once=True,
            retry=Retry(times=3),
        )
    assert "You cannot enable at_most_once and have retries" in str(err)


def test_apply_async_expires(task_namespace: TaskNamespace) -> None:
    def test_func(*args: Any, **kwargs: Any) -> None:
        pass

    task = Task(
        name="test.test_func",
        func=test_func,
        namespace=task_namespace,
    )
    with patch.object(task_namespace, "send_task") as mock_send:
        task.apply_async(args=["arg2"], kwargs={"org_id": 2}, expires=10, producer=None)
        assert mock_send.call_count == 1
        call_params = mock_send.call_args

    activation = call_params.args[0]
    assert activation.expires == 10
    expected_params = {"args": ["arg2"], "kwargs": {"org_id": 2}}
    assert msgpack.unpackb(activation.parameters_bytes, raw=False) == expected_params
    assert orjson.loads(activation.parameters) == expected_params


def test_apply_async_countdown(task_namespace: TaskNamespace) -> None:
    def test_func(*args: Any, **kwargs: Any) -> None:
        pass

    task = Task(
        name="test.test_func",
        func=test_func,
        namespace=task_namespace,
    )
    with patch.object(task_namespace, "send_task") as mock_send:
        task.apply_async(args=["arg2"], kwargs={"org_id": 2}, countdown=600, producer=None)
        assert mock_send.call_count == 1
        call_params = mock_send.call_args

    activation = call_params.args[0]
    assert activation.delay == 600
    expected_params = {"args": ["arg2"], "kwargs": {"org_id": 2}}
    assert msgpack.unpackb(activation.parameters_bytes, raw=False) == expected_params
    assert orjson.loads(activation.parameters) == expected_params


def test_delay_immediate_mode(task_namespace: TaskNamespace) -> None:
    calls = []

    def test_func(*args: Any, **kwargs: Any) -> None:
        calls.append({"args": args, "kwargs": kwargs})

    task = Task(
        name="test.test_func",
        func=test_func,
        namespace=task_namespace,
    )
    # Patch the constant that controls eager execution
    with patch("taskbroker_client.task.ALWAYS_EAGER", True):
        task.delay("arg", org_id=1)
        task.apply_async(args=["arg2"], kwargs={"org_id": 2})
        task.apply_async()

    assert len(calls) == 3
    assert calls[0] == {"args": ("arg",), "kwargs": {"org_id": 1}}
    assert calls[1] == {"args": ("arg2",), "kwargs": {"org_id": 2}}
    assert calls[2] == {"args": tuple(), "kwargs": {}}


def test_delay_immediate_validate_activation(task_namespace: TaskNamespace) -> None:
    calls = []

    def test_func(mixed: Any) -> None:
        calls.append({"mixed": mixed})

    task = Task(
        name="test.test_func",
        func=test_func,
        namespace=task_namespace,
    )

    with patch("taskbroker_client.task.ALWAYS_EAGER", True):
        task.delay(mixed=None)
        task.delay(mixed="str")

        with pytest.raises(TypeError) as err:
            task.delay(mixed=datetime.timedelta(days=1))
            assert "not JSON serializable" in str(err)

    assert len(calls) == 2
    assert calls[0] == {"mixed": None}
    assert calls[1] == {"mixed": "str"}


def test_should_retry(task_namespace: TaskNamespace) -> None:
    retry = Retry(times=3, times_exceeded=LastAction.Deadletter)
    state = retry.initial_state()

    task = Task(
        name="test.do_things",
        func=do_things,
        namespace=task_namespace,
        retry=retry,
    )
    err = RetryTaskError("try again plz")
    assert task.should_retry(state, err)

    state.attempts = 3
    assert not task.should_retry(state, err)

    no_retry = Task(
        name="test.no_retry",
        func=do_things,
        namespace=task_namespace,
        retry=None,
    )
    assert not no_retry.should_retry(state, err)


def test_create_activation(task_namespace: TaskNamespace) -> None:
    no_retry_task = Task(
        name="test.no_retry",
        func=do_things,
        namespace=task_namespace,
        retry=None,
    )

    retry = Retry(times=3, times_exceeded=LastAction.Deadletter)
    retry_task = Task(
        name="test.with_retry",
        func=do_things,
        namespace=task_namespace,
        retry=retry,
    )

    timedelta_expiry_task = Task(
        name="test.with_timedelta_expires",
        func=do_things,
        namespace=task_namespace,
        expires=datetime.timedelta(minutes=5),
        processing_deadline_duration=datetime.timedelta(seconds=30),
    )
    int_expiry_task = Task(
        name="test.with_int_expires",
        func=do_things,
        namespace=task_namespace,
        expires=5 * 60,
        processing_deadline_duration=30,
    )

    at_most_once_task = Task(
        name="test.at_most_once",
        func=do_things,
        namespace=task_namespace,
        at_most_once=True,
    )
    # No retries will be made as there is no retry policy on the task or namespace.
    activation = no_retry_task.create_activation([], {})
    assert activation.application == "acme"
    assert activation.taskname == "test.no_retry"
    assert activation.namespace == task_namespace.name
    assert activation.retry_state
    assert activation.retry_state.attempts == 0
    assert activation.retry_state.max_attempts == 1
    assert activation.retry_state.on_attempts_exceeded == ON_ATTEMPTS_EXCEEDED_DISCARD

    activation = retry_task.create_activation([], {})
    assert activation.application == "acme"
    assert activation.taskname == "test.with_retry"
    assert activation.namespace == task_namespace.name
    assert activation.retry_state
    assert activation.retry_state.attempts == 0
    assert activation.retry_state.max_attempts == 3
    assert activation.retry_state.on_attempts_exceeded == ON_ATTEMPTS_EXCEEDED_DEADLETTER

    activation = timedelta_expiry_task.create_activation([], {})
    assert activation.application == "acme"
    assert activation.taskname == "test.with_timedelta_expires"
    assert activation.expires == 300
    assert activation.processing_deadline_duration == 30

    activation = int_expiry_task.create_activation([], {})
    assert activation.application == "acme"
    assert activation.taskname == "test.with_int_expires"
    assert activation.expires == 300
    assert activation.processing_deadline_duration == 30

    activation = int_expiry_task.create_activation([], {}, expires=600)
    assert activation.application == "acme"
    assert activation.taskname == "test.with_int_expires"
    assert activation.expires == 600
    assert activation.processing_deadline_duration == 30

    activation = at_most_once_task.create_activation([], {})
    assert activation.application == "acme"
    assert activation.taskname == "test.at_most_once"
    assert activation.namespace == task_namespace.name
    assert activation.retry_state
    assert activation.retry_state.at_most_once is True
    assert activation.retry_state.attempts == 0
    assert activation.retry_state.max_attempts == 1
    assert activation.retry_state.on_attempts_exceeded == ON_ATTEMPTS_EXCEEDED_DISCARD


def test_create_activation_parameters(task_namespace: TaskNamespace) -> None:
    @task_namespace.register(name="test.parameters")
    def with_parameters(one: str, two: int, org_id: int) -> None:
        raise NotImplementedError

    activation = with_parameters.create_activation(["one", 22], {"org_id": 99})
    params = msgpack.unpackb(activation.parameters_bytes, raw=False)
    assert params["args"]
    assert params["args"] == ["one", 22]
    assert params["kwargs"] == {"org_id": 99}

    json_params = orjson.loads(activation.parameters)
    assert json_params == params


def test_create_activation_tracing(task_namespace: TaskNamespace) -> None:
    @task_namespace.register(name="test.parameters")
    def with_parameters(one: str, two: int, org_id: int) -> None:
        raise NotImplementedError

    with sentry_sdk.start_transaction(op="test.task"):
        activation = with_parameters.create_activation(["one", 22], {"org_id": 99})

    headers = activation.headers
    assert headers["sentry-trace"]
    assert "baggage" in headers


def test_create_activation_tracing_headers(task_namespace: TaskNamespace) -> None:
    @task_namespace.register(name="test.parameters")
    def with_parameters(one: str, two: int, org_id: int) -> None:
        raise NotImplementedError

    with sentry_sdk.start_transaction(op="test.task"):
        activation = with_parameters.create_activation(
            ["one", 22], {"org_id": 99}, {"key": "value"}
        )

    headers = activation.headers
    assert headers["sentry-trace"]
    assert "baggage" in headers
    assert headers["key"] == "value"


def test_create_activation_tracing_disable(task_namespace: TaskNamespace) -> None:
    @task_namespace.register(name="test.parameters")
    def with_parameters(one: str, two: int, org_id: int) -> None:
        raise NotImplementedError

    with sentry_sdk.start_transaction(op="test.task"):
        activation = with_parameters.create_activation(
            ["one", 22], {"org_id": 99}, {"sentry-propagate-traces": False}
        )

    headers = activation.headers
    assert "sentry-trace" not in headers
    assert "baggage" not in headers


def test_create_activation_headers_scalars(task_namespace: TaskNamespace) -> None:
    @task_namespace.register(name="test.parameters")
    def with_parameters(one: str, two: int, org_id: int) -> None:
        raise NotImplementedError

    headers = {
        "str": "value",
        "int": 22,
        "float": 3.14,
        "bool": False,
        "none": None,
    }
    activation = with_parameters.create_activation(["one", 22], {"org_id": 99}, headers)
    assert activation.headers["str"] == "value"
    assert activation.headers["int"] == "22"
    assert activation.headers["float"] == "3.14"
    assert activation.headers["bool"] == "False"
    assert activation.headers["none"] == "None"


def test_create_activation_headers_nested(task_namespace: TaskNamespace) -> None:
    @task_namespace.register(name="test.parameters")
    def with_parameters(one: str, two: int, org_id: int) -> None:
        raise NotImplementedError

    headers = {
        "key": "value",
        "nested": {
            "name": "sentry",
        },
    }
    with pytest.raises(ValueError) as err:
        with_parameters.create_activation(["one", 22], {"org_id": 99}, headers)
    assert "Only scalar header values are supported" in str(err)
    assert "The `nested` header value is of type <class 'dict'>" in str(err)


def test_create_activation_headers_monitor_config_treatment(task_namespace: TaskNamespace) -> None:
    @task_namespace.register(name="test.parameters")
    def with_parameters(one: str, two: int, org_id: int) -> None:
        raise NotImplementedError

    headers = {
        "key": "value",
        "sentry-monitor-config": {
            "schedule": {"type": "crontab", "value": "*/15 * * * *"},
            "timezone": "UTC",
        },
        "sentry-monitor-slug": "delete-stuff",
        "sentry-monitor-check-in-id": "abc123",
    }
    activation = with_parameters.create_activation(["one", 22], {"org_id": 99}, headers)

    result = activation.headers
    assert result
    assert result["key"] == "value"
    assert "sentry-monitor-config" not in result
    assert "sentry-monitor-slug" in result
    assert "sentry-monitor-check-in-id" in result


class StubContextHook:
    """Test hook that writes/reads a simple key."""

    last_executed: str

    def on_dispatch(self, headers: MutableMapping[str, Any]) -> None:
        headers["x-test-context"] = "dispatched"

    def on_execute(self, headers: dict[str, str]) -> contextlib.AbstractContextManager[None]:
        if "x-test-context" not in headers:
            return contextlib.nullcontext()
        # Store the value so the test can verify it was called
        StubContextHook.last_executed = headers["x-test-context"]
        return contextlib.nullcontext()


def test_context_hook_on_dispatch() -> None:
    """Context hooks inject headers during create_activation."""
    ns = TaskNamespace(
        name="tests",
        application="acme",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
        retry=None,
        context_hooks=[StubContextHook()],
    )

    @ns.register(name="test.hooked")
    def hooked_task() -> None:
        pass

    activation = hooked_task.create_activation([], {})
    assert activation.headers["x-test-context"] == "dispatched"


def test_context_hook_not_present_without_hooks() -> None:
    """Without hooks, no extra headers are injected."""
    ns = TaskNamespace(
        name="tests",
        application="acme",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
        retry=None,
    )

    @ns.register(name="test.no_hooks")
    def no_hooks_task() -> None:
        pass

    activation = no_hooks_task.create_activation([], {})
    assert "x-test-context" not in activation.headers


def test_context_hook_multiple_hooks() -> None:
    """Multiple hooks all get called."""

    class AnotherHook:
        def on_dispatch(self, headers: MutableMapping[str, Any]) -> None:
            headers["x-another"] = "also-here"

        def on_execute(self, headers: dict[str, str]) -> contextlib.AbstractContextManager[None]:
            return contextlib.nullcontext()

    ns = TaskNamespace(
        name="tests",
        application="acme",
        producer_factory=producer_factory,
        router=DefaultRouter(),
        metrics=NoOpMetricsBackend(),
        retry=None,
        context_hooks=[StubContextHook(), AnotherHook()],
    )

    @ns.register(name="test.multi_hooks")
    def multi_task() -> None:
        pass

    activation = multi_task.create_activation([], {})
    assert activation.headers["x-test-context"] == "dispatched"
    assert activation.headers["x-another"] == "also-here"


def test_task_pass_headers_attribute(task_namespace: TaskNamespace) -> None:
    """Tasks can opt into receiving headers via pass_headers=True."""

    @task_namespace.register(name="test.with_headers", pass_headers=True)
    def with_headers(org_id: int, headers: dict[str, str]) -> None:
        pass

    assert with_headers.pass_headers is True

    @task_namespace.register(name="test.without_headers")
    def without_headers(org_id: int) -> None:
        pass

    assert without_headers.pass_headers is False


def test_pass_headers_requires_headers_parameter(task_namespace: TaskNamespace) -> None:
    """Tasks with pass_headers=True must have a 'headers' parameter."""
    with pytest.raises(TypeError, match="does not have a 'headers' parameter"):

        @task_namespace.register(name="test.missing_headers", pass_headers=True)
        def missing_headers(org_id: int) -> None:
            pass


def test_pass_headers_rejects_positional_only_headers(task_namespace: TaskNamespace) -> None:
    """Tasks with pass_headers=True cannot have a positional-only 'headers' parameter."""
    with pytest.raises(TypeError, match="positional-only"):

        @task_namespace.register(name="test.positional_headers", pass_headers=True)
        def positional_headers(org_id: int, headers: dict[str, str], /) -> None:
            pass


def test_pass_headers_rejects_incompatible_type_annotation(
    task_namespace: TaskNamespace,
) -> None:
    """Tasks with pass_headers=True must have a dict-like type annotation for 'headers'."""
    with pytest.raises(TypeError, match="Expected one of: dict"):

        @task_namespace.register(name="test.wrong_type_headers", pass_headers=True)
        def wrong_type_headers(org_id: int, headers: str) -> None:
            pass


def test_delay_immediate_mode_with_pass_headers(task_namespace: TaskNamespace) -> None:
    """In ALWAYS_EAGER mode, tasks with pass_headers=True receive empty headers."""
    calls: list[dict[str, Any]] = []

    @task_namespace.register(name="test.headers_task", pass_headers=True)
    def headers_task(value: str, headers: dict[str, str]) -> None:
        calls.append({"value": value, "headers": headers})

    with patch("taskbroker_client.task.ALWAYS_EAGER", True):
        headers_task.delay("test")  # type: ignore[call-arg]

    assert len(calls) == 1
    assert calls[0]["value"] == "test"
    assert calls[0]["headers"] == {}


def test_delay_immediate_mode_without_pass_headers(task_namespace: TaskNamespace) -> None:
    """In ALWAYS_EAGER mode, tasks without pass_headers do not receive headers kwarg."""
    calls: list[dict[str, Any]] = []

    @task_namespace.register(name="test.no_headers_task")
    def no_headers_task(value: str) -> None:
        calls.append({"value": value})

    with patch("taskbroker_client.task.ALWAYS_EAGER", True):
        no_headers_task.delay("test")

    assert len(calls) == 1
    assert calls[0] == {"value": "test"}
