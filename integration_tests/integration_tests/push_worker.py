"""
Minimal mock worker for push-mode integration tests.

In push mode the taskbroker is the gRPC *client*: it claims activations from the
store and pushes each one to a worker's `WorkerService.PushTask` endpoint (see
`src/worker.rs`, `src/push/`). This module provides a lightweight gRPC *server*
implementing that endpoint so tests can observe what the broker pushes.

This is deliberately minimal (records pushed task ids, optionally fails) rather
than reusing the production `PushTaskWorker` in `clients/python`, which spawns
child processes and needs an app module — mirroring how `worker.py`'s
`ConfigurableTaskWorker` is a lightweight mock of the pull-mode worker.
"""

import threading
import time
from concurrent import futures

import grpc
from sentry_protos.taskbroker.v1 import taskbroker_pb2_grpc
from sentry_protos.taskbroker.v1.taskbroker_pb2 import PushTaskRequest, PushTaskResponse


class MockWorkerServicer(taskbroker_pb2_grpc.WorkerServiceServicer):
    """
    Records every activation id pushed by the broker. When `fail` is set, every
    push is rejected so the broker exercises its revert-claimed-to-pending path.
    """

    def __init__(self, fail: bool = False, hang: float = 0.0) -> None:
        self.fail = fail
        # Seconds to block inside PushTask before responding. When larger than
        # the broker's push.timeout, the push RPC times out on the broker side;
        # this keeps the push pipeline saturated so claimed tasks pile up and
        # are reverted by the upkeep claim-expiration path rather than the push
        # thread.
        self.hang = hang
        self._lock = threading.Lock()
        # All pushed ids in order (includes duplicates so tests can detect them).
        self.pushed: list[str] = []

    def PushTask(
        self,
        request: PushTaskRequest,
        context: grpc.ServicerContext,
    ) -> PushTaskResponse:
        with self._lock:
            self.pushed.append(request.task.id)

        if self.hang:
            time.sleep(self.hang)

        if self.fail:
            context.abort(grpc.StatusCode.UNAVAILABLE, "mock worker rejecting push")

        return PushTaskResponse()

    def pushed_ids(self) -> list[str]:
        with self._lock:
            return list(self.pushed)

    def unique_pushed_ids(self) -> set[str]:
        with self._lock:
            return set(self.pushed)


def start_mock_worker(
    port: int, fail: bool = False, hang: float = 0.0
) -> tuple[grpc.Server, MockWorkerServicer]:
    """
    Start a `WorkerService` gRPC server on `port` bound to localhost.

    Returns the running server and its servicer; the caller is responsible for
    `server.stop(grace)` when the test finishes. Bootstrap mirrors
    `clients/python/src/taskbroker_client/worker/worker.py`.
    """
    servicer = MockWorkerServicer(fail=fail, hang=hang)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    taskbroker_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    print(f"[mock_worker] Listening on 127.0.0.1:{port} (fail={fail})")
    return server, servicer
