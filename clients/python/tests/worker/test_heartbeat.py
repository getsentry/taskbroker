import queue
from unittest import mock

import grpc
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    BatchUpdateHeartbeatResponse,
    PushTaskRequest,
    TaskActivation,
)

from taskbroker_client.metrics import NoOpMetricsBackend
from taskbroker_client.worker.heartbeat import HeartbeatCommand, HeartbeatTracker, heartbeat_process
from taskbroker_client.worker.push_clients import PushTaskbrokerClient
from taskbroker_client.worker.worker import PushTaskWorker, WorkerServicer


class FakeEvent:
    """Lets `heartbeat_process` and `_supervise` run a fixed number of iterations."""

    def __init__(self, iterations: int) -> None:
        self.iterations = iterations

    def wait(self, timeout: float) -> bool:
        self.iterations -= 1
        return self.iterations < 0

    def is_set(self) -> bool:
        return self.iterations < 0


class UnavailableError(grpc.RpcError):
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE


def test_update_heartbeats_returns_not_found_ids() -> None:
    client = PushTaskbrokerClient(
        service="localhost:50051", application="tests", metrics=NoOpMetricsBackend()
    )
    with mock.patch.object(client, "_stub") as stub:
        stub.BatchUpdateHeartbeat.return_value = BatchUpdateHeartbeatResponse(not_found_ids=["b"])
        assert client.update_heartbeats(["a", "b"]) == ["b"]

    (request,), _ = stub.BatchUpdateHeartbeat.call_args
    assert list(request.ids) == ["a", "b"]


def test_heartbeat_process_applies_commands_and_drops_not_found() -> None:
    commands: queue.Queue[HeartbeatCommand] = queue.Queue()
    commands.put(("track", ["b", "c"]))
    commands.put(("untrack", ["c"]))

    with (
        mock.patch("taskbroker_client.worker.heartbeat.signal.signal"),
        mock.patch.object(
            PushTaskbrokerClient,
            "update_heartbeats",
            side_effect=[["a"], UnavailableError(), []],
        ) as update,
    ):
        heartbeat_process(
            "examples.app:app", "localhost:50051", "test", ["a"], commands, FakeEvent(3)  # type: ignore[arg-type]
        )

    assert [sorted(c.args[0]) for c in update.call_args_list] == [["a", "b"], ["b"], ["b"]]


def _tracker(mp_context: mock.MagicMock) -> HeartbeatTracker:
    return HeartbeatTracker(
        app_module="examples.app:app",
        broker_service="localhost:50051",
        processing_pool_name="test",
        mp_context=mp_context,
        metrics=NoOpMetricsBackend(),
    )


def test_tracker_restarts_dead_process_with_tracked_ids() -> None:
    mp_context = mock.MagicMock()
    tracker = _tracker(mp_context)
    tracker.track("a")
    tracker.track("b")
    tracker.untrack(["a", "missing"])

    # First tick starts it, second tick sees it alive (MagicMock) and leaves it.
    tracker._shutdown_event = FakeEvent(2)  # type: ignore[assignment]
    tracker._supervise()
    assert mp_context.Process.call_count == 1
    assert mp_context.Process.call_args.kwargs["args"][3] == ["b"]

    mp_context.Process.return_value.is_alive.return_value = False
    tracker._shutdown_event = FakeEvent(1)  # type: ignore[assignment]
    tracker._supervise()
    assert mp_context.Process.call_count == 2


def test_tracker_does_not_start_without_tracked_ids() -> None:
    mp_context = mock.MagicMock()
    tracker = _tracker(mp_context)
    tracker._shutdown_event = FakeEvent(2)  # type: ignore[assignment]
    tracker._supervise()
    mp_context.Process.assert_not_called()


def _push_worker() -> PushTaskWorker:
    return PushTaskWorker(
        app_module="examples.app:app",
        broker_service="127.0.0.1:50051",
        max_child_task_count=100,
        process_type="fork",
        drain_file_path=None,
    )


def test_push_task_tracks_heartbeat_tasks_only() -> None:
    taskworker = _push_worker()
    servicer = WorkerServicer(taskworker.worker_pool, heartbeats=taskworker._heartbeats)

    with mock.patch.object(taskworker.worker_pool, "push_task", return_value=True):
        servicer.PushTask(
            PushTaskRequest(task=TaskActivation(id="beat", heartbeat=10)), mock.MagicMock()
        )
        servicer.PushTask(PushTaskRequest(task=TaskActivation(id="plain")), mock.MagicMock())

    assert taskworker._heartbeats._ids == {"beat"}

    with mock.patch.object(taskworker.worker_pool, "push_task", return_value=False):
        servicer.PushTask(
            PushTaskRequest(task=TaskActivation(id="busy", heartbeat=10)), mock.MagicMock()
        )

    assert taskworker._heartbeats._ids == {"beat"}


def test_send_results_untracks_sent_tasks() -> None:
    taskworker = _push_worker()
    taskworker._heartbeats.track("a")
    result = mock.MagicMock(task_id="a", receive_timestamp=0)

    with mock.patch.object(taskworker.client, "update_tasks"):
        taskworker._send_results([result])

    assert taskworker._heartbeats._ids == set()
