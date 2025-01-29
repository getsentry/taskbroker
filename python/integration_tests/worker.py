import grpc
import time
import random
import logging

from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    TaskActivation,
    FetchNextTask,
    GetTaskRequest,
    SetTaskStatusRequest,
    TaskActivationStatus,
    TASK_ACTIVATION_STATUS_COMPLETE,
    TASK_ACTIVATION_STATUS_FAILURE,
    TASK_ACTIVATION_STATUS_RETRY
)
from sentry_protos.taskbroker.v1.taskbroker_pb2_grpc import ConsumerServiceStub


class TaskWorkerClient:
    """
    Taskworker RPC client wrapper
    """

    def __init__(self, host: str) -> None:
        self._host = host
        self._channel = grpc.insecure_channel(self._host)
        self._stub = ConsumerServiceStub(self._channel)

    def get_task(self, namespace: str | None = None) -> TaskActivation | None:
        """
        Fetch a pending task.

        If a namespace is provided, only tasks for that namespace will be fetched.
        This will return None if there are no tasks to fetch.
        """
        request = GetTaskRequest(namespace=namespace)
        try:
            response = self._stub.GetTask(request)
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise
        if response.HasField("task"):
            return response.task
        return None

    def update_task(
        self, task_id: str, status: TaskActivationStatus.ValueType, fetch_next_task: FetchNextTask | None
    ) -> TaskActivation | None:
        """
        Update the status for a given task activation.

        The return value is the next task that should be executed.
        """
        request = SetTaskStatusRequest(
            id=task_id,
            status=status,
            fetch_next_task=fetch_next_task,
        )
        try:
            response = self._stub.SetTaskStatus(request)
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise
        if response.HasField("task"):
            return response.task
        return None


class ConfigurableTaskWorker:
    """
    A taskworker that can be configured to fail/timeout while processing tasks.
    """

    def __init__(
        self,
        client: TaskWorkerClient,
        namespace: str | None = None,
        failure_rate: float = 0.0,
        timeout_rate: float = 0.0,
        retry_rate: float = 0.0,
        enable_backoff: bool = False,
    ) -> None:
        self.client = client
        self._namespace: str | None = namespace
        self._failure_rate: float = failure_rate
        self._timeout_rate: float = timeout_rate
        self._retry_rate: float = retry_rate
        self._backoff_wait_time: float | None = 0.0 if enable_backoff else None

    def fetch_task(self) -> TaskActivation | None:
        try:
            activation = self.client.get_task(self._namespace)
        except grpc.RpcError as err:
            logging.error(f"get_task failed. Retrying in 1 second: {err}")
            time.sleep(1)
            return None

        if not activation:
            logging.debug("No task fetched")
            if self._backoff_wait_time is not None:
                logging.debug(f"Backing off for {self._backoff_wait_time} seconds")
                time.sleep(self._backoff_wait_time)
                self._backoff_wait_time = min(self._backoff_wait_time + 1, 10)
            return None

        self._backoff_wait_time = 0.0
        return activation

    def process_task(self, activation: TaskActivation) -> TaskActivation | None:
        logging.debug(f"Processing task {activation.id}")
        update_status = TASK_ACTIVATION_STATUS_COMPLETE

        if self._timeout_rate and random.random() < self._timeout_rate:
            return None  # Pretend that the task was dropped

        if self._failure_rate and random.random() < self._failure_rate:
            update_status = TASK_ACTIVATION_STATUS_FAILURE

        if self._retry_rate and random.random() < self._retry_rate:
            update_status = TASK_ACTIVATION_STATUS_RETRY

        return self.client.update_task(
            task_id=activation.id,
            status=update_status,
            fetch_next_task=FetchNextTask(namespace=self._namespace),
        )

    def complete_task(self, activation: TaskActivation) -> TaskActivation | None:
        return self.client.update_task(
            task_id=activation.id,
            status=TASK_ACTIVATION_STATUS_COMPLETE,
            fetch_next_task=FetchNextTask(namespace=self._namespace),
        )
