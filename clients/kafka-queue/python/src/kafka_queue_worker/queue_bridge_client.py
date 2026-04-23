from __future__ import annotations

import logging
import time
from typing import Any

import grpc
from kafka_queue_proto import queue_bridge_pb2, queue_bridge_pb2_grpc

from kafka_queue_worker.types import (
    BRIDGE_HOST_TAG,
    InflightTaskActivation,
    NoOpMetrics,
    ProcessingResult,
    TaskActivation,
    FetchNextTask,
)

logger = logging.getLogger(__name__)

MAX_ACT = 10 * 1024 * 1024
OPTIONS: list[tuple[str, Any]] = [("grpc.max_receive_message_length", MAX_ACT), ("grpc.max_send_message_length", MAX_ACT)]


class QueueBridgeClient:
    """gRPC to Java KafkaShareConsumer bridge. Drop-in for the *subset* of TaskworkerWorker logic used by TaskQueueWorker."""

    def __init__(
        self,
        bridge_addr: str,
        metrics: NoOpMetrics,
        application: str = "kafka-queue-proto",
    ) -> None:
        self._application = application
        self._addr = bridge_addr
        self._metrics = metrics
        self._channel = grpc.insecure_channel(bridge_addr, options=OPTIONS)
        self._stub = queue_bridge_pb2_grpc.QueueBridgeStub(self._channel)
        self._lock_host = BRIDGE_HOST_TAG
        logger.info("queue-bridge connect %s", bridge_addr)

    @property
    def application(self) -> str:
        return self._application

    def get_task(self, namespace: str | None = None) -> InflightTaskActivation | None:
        return self._poll()

    def _poll(self) -> InflightTaskActivation | None:
        with self._metrics.timer("kqueue.poll.rpc"):
            r = self._stub.Poll(queue_bridge_pb2.PollRequest(poll_timeout_ms=5000))
        if r.empty or not r.delivery_id or not r.payload:
            return None
        a = TaskActivation()
        a.ParseFromString(r.payload)
        h = r.bridge_host or self._lock_host
        return InflightTaskActivation(
            activation=a,
            host=h,
            receive_timestamp=time.monotonic(),
            delivery_id=r.delivery_id,
        )

    def update_task(
        self, processing_result: ProcessingResult, fetch_next_task: FetchNextTask | None = None
    ) -> InflightTaskActivation | None:
        want_next = fetch_next_task is not None
        with self._metrics.timer("kqueue.complete.rpc"):
            r = self._stub.Complete(
                queue_bridge_pb2.CompleteRequest(
                    delivery_id=processing_result.delivery_id,
                    task_id=processing_result.task_id,
                    activation_status=int(processing_result.status),
                    fetch_next=want_next,
                    poll_timeout_ms=5000,
                )
            )
        if r.has_next and r.next_delivery_id and r.next_payload:
            a = TaskActivation()
            a.ParseFromString(bytes(r.next_payload))
            return InflightTaskActivation(
                activation=a,
                host=r.bridge_host or self._lock_host,
                receive_timestamp=time.monotonic(),
                delivery_id=r.next_delivery_id,
            )
        if want_next:
            return self._poll()
        return None

    def check(self) -> bool:
        try:
            c = self._stub.Check(queue_bridge_pb2.CheckRequest(), timeout=2.0)
            return c.ready
        except grpc.RpcError:
            return False

    def close(self) -> None:
        self._channel.close()
