import grpc
import time

from sentry_protos.sentry.v1.taskworker_pb2 import TaskActivation, FetchNextTask, GetTaskRequest, SetTaskStatusRequest, TaskActivationStatus, TASK_ACTIVATION_STATUS_COMPLETE, TASK_ACTIVATION_STATUS_RETRY
from sentry_protos.sentry.v1.taskworker_pb2_grpc import ConsumerServiceStub


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
        self, task_id: str, status: TaskActivationStatus.ValueType, fetch_next_task: FetchNextTask
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


class SimpleTaskWorker:
    """
    A simple TaskWorker that is used for integration tests. This taskworker does not
    actually execute tasks, it simply fetches tasks from the taskworker gRPC server
    and updates their status depending on the test scenario.
    """

    def __init__(self, client: TaskWorkerClient, namespace: str | None = None) -> None:
        self.client = client
        self._namespace: str | None = namespace

    def fetch_task(self) -> TaskActivation | None:
        try:
            activation = self.client.get_task(self._namespace)
        except grpc.RpcError:
            print("get_task failed. Retrying in 1 second")
            return None

        if not activation:
            print("No task fetched")
            return None

        return activation

    def process_task(self, activation: TaskActivation) -> TaskActivation | None:
        return self.client.update_task(
            task_id=activation.id,
            status=TASK_ACTIVATION_STATUS_COMPLETE,
            fetch_next_task=FetchNextTask(namespace=self._namespace),
        )

    def process_task_with_retry(self, activation: TaskActivation) -> TaskActivation | None:
        return self.client.update_task(
            task_id=activation.id,
            status=TASK_ACTIVATION_STATUS_RETRY,
            fetch_next_task=FetchNextTask(namespace=self._namespace),
        )
