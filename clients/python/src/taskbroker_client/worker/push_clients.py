import logging
import threading
import time
from typing import TYPE_CHECKING, Any

import grpc
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    SetBatchActivationStatusRequest,
    SetTaskStatusRequest,
)
from sentry_protos.taskbroker.v1.taskbroker_pb2_grpc import ConsumerServiceStub

from taskbroker_client.metrics import MetricsBackend
from taskbroker_client.types import ProcessingResult
from taskbroker_client.worker.client import (
    MAX_ACTIVATION_SIZE,
    HealthCheckSettings,
    RequestSignatureInterceptor,
    parse_rpc_secret_list,
)

if TYPE_CHECKING:
    ServerInterceptor = grpc.ServerInterceptor[Any, Any]
else:
    ServerInterceptor = grpc.ServerInterceptor

logger = logging.getLogger(__name__)


class PushTaskbrokerClient:
    """
    Taskworker RPC client wrapper

    Push brokers are a deployment so they don't need to be connected to individually. There is one service provided
    that works for all the brokers.
    """

    def __init__(
        self,
        service: str,
        application: str,
        metrics: MetricsBackend,
        health_check_settings: HealthCheckSettings | None = None,
        rpc_secret: str | None = None,
        grpc_config: str | None = None,
    ) -> None:
        self._application = application
        self._service = service
        self._rpc_secret = rpc_secret
        self._metrics = metrics

        self._grpc_options: list[tuple[str, Any]] = [
            ("grpc.max_receive_message_length", MAX_ACTIVATION_SIZE)
        ]
        if grpc_config:
            self._grpc_options.append(("grpc.service_config", grpc_config))

        logger.info(
            "taskworker.push_client.start",
            extra={"service": service, "options": self._grpc_options},
        )

        self._stub = self._connect_to_host(service)

        self._health_check_settings = health_check_settings
        self._timestamp_since_touch_lock = threading.Lock()
        self._timestamp_since_touch = 0.0

    def _emit_health_check(self) -> None:
        if self._health_check_settings is None:
            return

        with self._timestamp_since_touch_lock:
            cur_time = time.time()
            if (
                cur_time - self._timestamp_since_touch
                < self._health_check_settings.touch_interval_sec
            ):
                return

            self._health_check_settings.file_path.touch()
            self._metrics.incr(
                "taskworker.client.health_check.touched",
            )
            self._timestamp_since_touch = cur_time

    def _connect_to_host(self, host: str) -> ConsumerServiceStub:
        logger.info("taskworker.push_client.connect", extra={"host": host})
        channel = grpc.insecure_channel(host, options=self._grpc_options)
        secrets = parse_rpc_secret_list(self._rpc_secret)
        if secrets:
            channel = grpc.intercept_channel(channel, RequestSignatureInterceptor(secrets))
        return ConsumerServiceStub(channel)

    def emit_health_check(self) -> None:
        self._emit_health_check()

    def update_tasks(self, processing_results: list[ProcessingResult]) -> None:
        for processing_result in processing_results:
            self._update_task_single(processing_result)

    def _update_task_single(
        self,
        processing_result: ProcessingResult,
    ) -> None:
        """
        Update the status for a given task activation.
        """
        self._emit_health_check()

        request = SetTaskStatusRequest(
            id=processing_result.task_id,
            status=processing_result.status,
            fetch_next_task=None,
            max_attempts=processing_result.max_attempts,
            delay_on_retry=processing_result.delay_on_retry,
        )

        retries = 0
        exception = None
        while retries < 3:
            try:
                with self._metrics.timer(
                    "taskworker.update_task.rpc", tags={"service": self._service}
                ):
                    self._stub.SetTaskStatus(request)
                exception = None
                break
            except grpc.RpcError as err:
                exception = err
                self._metrics.incr(
                    "taskworker.client.rpc_error",
                    tags={"method": "SetTaskStatus", "status": err.code().name},
                )
            finally:
                retries += 1

        if exception:
            raise exception


class BatchPushTaskbrokerClient(PushTaskbrokerClient):
    """
    Taskworker RPC client wrapper

    Push brokers are a deployment so they don't need to be connected to individually. There is one service provided
    that works for all the brokers. This client pushes batches of activation updates.
    """

    def update_tasks(
        self,
        processing_results: list[ProcessingResult],
    ) -> None:
        """
        Update the status for a given task activation.
        """
        self._emit_health_check()

        request = SetBatchActivationStatusRequest(
            updates=[
                SetTaskStatusRequest(
                    id=processing_result.task_id,
                    status=processing_result.status,
                    fetch_next_task=None,
                    max_attempts=processing_result.max_attempts,
                    delay_on_retry=processing_result.delay_on_retry,
                )
                for processing_result in processing_results
            ]
        )

        retries = 0
        exception = None
        while retries < 3:
            try:
                with self._metrics.timer(
                    "taskworker.update_task_batch.rpc", tags={"service": self._service}
                ):
                    self._stub.SetBatchActivationStatus(request)
                exception = None
                break
            except grpc.RpcError as err:
                exception = err
                self._metrics.incr(
                    "taskworker.client.rpc_error",
                    tags={"method": "SetBatchActivationStatus", "status": err.code().name},
                )
            finally:
                retries += 1

        if exception:
            raise exception
