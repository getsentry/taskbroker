import dataclasses
import threading

from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation

_current_state = threading.local()


@dataclasses.dataclass
class CurrentTaskState:
    id: str
    namespace: str
    taskname: str
    attempt: int
    processing_deadline_duration: int
    retries_remaining: bool


def current_task() -> CurrentTaskState | None:
    if not hasattr(_current_state, "state"):
        _current_state.state = None

    return _current_state.state


def set_current_task(activation: TaskActivation, *, retries_remaining: bool) -> None:
    state = CurrentTaskState(
        id=activation.id,
        namespace=activation.namespace,
        taskname=activation.taskname,
        attempt=activation.retry_state.attempts,
        retries_remaining=retries_remaining,
        processing_deadline_duration=activation.processing_deadline_duration,
    )
    _current_state.state = state


def clear_current_task() -> None:
    _current_state.state = None
