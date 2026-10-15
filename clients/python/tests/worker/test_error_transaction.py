import queue
import signal
from multiprocessing import Event, get_context
from typing import Literal
from unittest import mock
from uuid import uuid4

import msgpack
import pytest
import sentry_sdk
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    TASK_ACTIVATION_STATUS_FAILURE,
    TaskActivation,
)
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.transport import Transport
from sentry_sdk.types import Event as SentryEvent

from taskbroker_client.app import TaskbrokerApp
from taskbroker_client.retry import Retry
from taskbroker_client.types import InflightTaskActivation, ProcessingResult
from taskbroker_client.worker.childtiming import SLOT_WIDTH
from taskbroker_client.worker.workerchild import ProcessingDeadlineExceeded, child_process


class RecordingTransport(Transport):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[SentryEvent] = []

    def capture_envelope(self, envelope: Envelope) -> None:
        for item in envelope.items:
            event = item.get_event()
            if event is not None:
                self.events.append(event)


@pytest.mark.parametrize("trace_lifecycle", ["static", "stream"])
@pytest.mark.parametrize("failure", ["timeout", "exception", "retry_exhausted"])
def test_task_error_transaction_overrides_stale_current_scope(
    trace_lifecycle: Literal["static", "stream"], failure: str
) -> None:
    transport = RecordingTransport()
    client = sentry_sdk.Client(
        dsn="https://public@example.com/1",
        default_integrations=False,
        integrations=[LoggingIntegration()],
        transport=transport,
        traces_sample_rate=0,
        trace_lifecycle=trace_lifecycle,
    )
    app = TaskbrokerApp(name="test", producer_factory=mock.Mock())
    namespace = app.create_namespace("tests")
    taskname = f"tests.{failure}"

    @namespace.register(
        name=taskname, retry=Retry(times=1) if failure == "retry_exhausted" else None
    )
    def failing_task() -> None:
        if failure == "timeout":
            raise ProcessingDeadlineExceeded("synthetic timeout")
        raise ValueError("synthetic failure")

    activation = TaskActivation(
        id="test-task",
        namespace="tests",
        taskname=taskname,
        parameters_bytes=msgpack.packb({"args": [], "kwargs": {}}, use_bin_type=True),
        processing_deadline_duration=30,
    )
    todo: queue.Queue[InflightTaskActivation] = queue.Queue()
    processed: queue.Queue[ProcessingResult] = queue.Queue()
    todo.put(
        InflightTaskActivation(host="localhost:50051", receive_timestamp=0, activation=activation)
    )

    ctx = get_context("fork")
    messages = ctx.Queue()
    parent_release = ctx.Event()
    parent_release.set()
    timing_shm = ctx.RawArray("d", SLOT_WIDTH)

    original_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        with sentry_sdk.isolation_scope() as isolation_scope:
            isolation_scope.set_client(client)
            sentry_sdk.get_current_scope().set_transaction_name("StartupService.load_packages")
            with mock.patch("taskbroker_client.worker.workerchild.import_app", return_value=app):
                child_process(
                    uuid4(),
                    "test:app",
                    todo,
                    processed,
                    Event(),
                    max_task_count=1,
                    processing_pool_name="test",
                    process_type="fork",
                    skip_awaiting_futures=False,
                    future_checking_frequency=0.1,
                    messages=messages,
                    parent_release=parent_release,
                    timing_shm=timing_shm,
                )

            assert sentry_sdk.get_current_scope()._transaction == "StartupService.load_packages"
            assert isolation_scope._transaction is None
    finally:
        for sig, handler in original_handlers.items():
            signal.signal(sig, handler)
        client.close()
        messages.close()
        messages.join_thread()

    assert processed.get_nowait().status == TASK_ACTIVATION_STATUS_FAILURE
    assert len(transport.events) == 1
    event = transport.events[0]
    assert event["transaction"] == taskname
    if failure in ("timeout", "retry_exhausted"):
        fingerprint = (
            "taskworker.processing_deadline_exceeded"
            if failure == "timeout"
            else "taskworker.no_retries_remaining"
        )
        assert event["fingerprint"] == [fingerprint, "tests", taskname]
    else:
        assert event["extra"]["taskname"] == taskname
