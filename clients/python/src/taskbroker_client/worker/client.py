import hashlib
import hmac
import logging
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Sequence, Tuple, Union

import grpc
import orjson
from google.protobuf.message import Message
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    FetchNextTask,
    GetTaskRequest,
    SetTaskStatusRequest,
)
from sentry_protos.taskbroker.v1.taskbroker_pb2_grpc import ConsumerServiceStub

from taskbroker_client.constants import (
    DEFAULT_CONSECUTIVE_UNAVAILABLE_ERRORS,
    DEFAULT_REBALANCE_AFTER,
    DEFAULT_TEMPORARY_UNAVAILABLE_HOST_TIMEOUT,
)
from taskbroker_client.metrics import MetricsBackend
from taskbroker_client.types import InflightTaskActivation, ProcessingResult

if TYPE_CHECKING:
    ServerInterceptor = grpc.ServerInterceptor[Any, Any]
else:
    ServerInterceptor = grpc.ServerInterceptor

logger = logging.getLogger(__name__)

# gRPC runs the unary request deserializer and the servicer on the same thread for a given call
# If HMAC verification fails inside a deserializer wrapper, raising turns into INTERNAL, so we set this flag and abort UNAUTHENTICATED from the servicer
_RPC_SIGNATURE_AUTH_TLS = threading.local()

MAX_ACTIVATION_SIZE = 1024 * 1024 * 10
"""Max payload size we will process."""


def make_broker_hosts(
    host_prefix: str,
    num_brokers: int | None,
    host_list: str | None = None,
) -> list[str]:
    """
    Handle RPC host CLI options and create a list of broker host:ports
    """
    if host_list:
        stripped = map(lambda x: x.strip(), host_list.split(","))
        return list(filter(lambda x: len(x), stripped))
    if not num_brokers:
        return [host_prefix]
    domain, port = host_prefix.split(":")
    return [f"{domain}-{i}:{port}" for i in range(0, num_brokers)]


class ClientCallDetails(grpc.ClientCallDetails):
    """
    Subclass of grpc.ClientCallDetails that allows metadata to be updated
    """

    def __init__(
        self,
        method: str,
        timeout: float | None,
        metadata: tuple[tuple[str, str | bytes], ...] | None,
        credentials: grpc.CallCredentials | None,
    ):
        self.timeout = timeout
        self.method = method
        self.metadata = metadata
        self.credentials = credentials


if TYPE_CHECKING:
    RpcMethodHandler = grpc.RpcMethodHandler[Any, Any]
    InterceptorBase = grpc.UnaryUnaryClientInterceptor[Message, Message]
    CallFuture = grpc.CallFuture[Message]
else:
    RpcMethodHandler = grpc.RpcMethodHandler
    InterceptorBase = grpc.UnaryUnaryClientInterceptor
    CallFuture = Any

ClientContinuation = Callable[[ClientCallDetails, Message], Any]
ServerContinuation = Callable[[grpc.HandlerCallDetails], Optional[RpcMethodHandler]]
Metadata = Sequence[Tuple[str, Union[str, bytes]]]


class RequestSignatureInterceptor(InterceptorBase):
    def __init__(self, shared_secret: list[str]):
        self._secret = shared_secret[0].encode("utf-8")

    def intercept_unary_unary(
        self,
        continuation: ClientContinuation,
        client_call_details: grpc.ClientCallDetails,
        request: Message,
    ) -> CallFuture:
        request_body = request.SerializeToString()
        method = client_call_details.method.encode("utf-8")

        signing_payload = method + b":" + request_body
        signature = hmac.new(self._secret, signing_payload, hashlib.sha256).hexdigest()

        metadata = list(client_call_details.metadata) if client_call_details.metadata else []
        metadata.append(("sentry-signature", signature))

        call_details_with_meta = ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            tuple(metadata),
            client_call_details.credentials,
        )
        return continuation(call_details_with_meta, request)


def parse_rpc_secret_list(rpc_secret: str | None) -> list[str] | None:
    """
    Parse the task app `rpc_secret` JSON array string into a list of secrets.
    Returns `None` when unset, invalid, or empty (no authentication).
    """
    if not rpc_secret:
        return None

    # Try to parse the provided secret
    parsed = orjson.loads(rpc_secret)

    if not isinstance(parsed, list) or len(parsed) == 0:
        # If the secret string is not a list with at least one element, it is invalid
        return None

    return [str(x) for x in parsed]


def grpc_metadata_get(metadata: Metadata, key: str) -> str | None:
    """
    First matching gRPC metadata value for `key` or `None` if not present.
    """
    # gRPC metadata keys are ASCII and compared case-insensitively
    key = key.lower()

    for k, v in metadata:
        if k.lower() == key:
            return v.decode("utf-8") if isinstance(v, bytes) else v

    return None


def verify_rpc_request_signature_hmac(
    secrets: list[str],
    method: str,
    request_body: bytes,
    signature_hex: str | None,
) -> bool:
    """
    Verify the 'sentry-signature' metadata for a unary RPC body.
    Uses the same signing contract as `RequestSignatureInterceptor` and the taskbroker.
    """
    if not secrets:
        return True

    if not signature_hex:
        return False

    try:
        sig_bytes = bytes.fromhex(signature_hex)
    except ValueError:
        return False

    signing_payload = method.encode("utf-8") + b":" + request_body

    for secret in secrets:
        expected = hmac.new(secret.encode("utf-8"), signing_payload, hashlib.sha256).digest()
        if hmac.compare_digest(expected, sig_bytes):
            return True

    return False


class RequestSignatureServerInterceptor(ServerInterceptor):
    """
    Enforces HMAC request signing on unary-unary RPCs like `WorkerService.PushTask`.
    Verification uses the raw request bytes from the wire (via a wrapped deserializer)
    so it stays consistent across languages and map encodings.
    """

    def __init__(self, secrets: list[str]) -> None:
        self._secrets = secrets

    def intercept_service(
        self, continuation: ServerContinuation, handler_call_details: grpc.HandlerCallDetails
    ) -> Any:
        handler = continuation(handler_call_details)
        if handler is None or not self._secrets:
            return handler

        if handler.request_streaming or handler.response_streaming or handler.unary_unary is None:
            return handler

        inner_deserializer = handler.request_deserializer
        if inner_deserializer is None:
            return handler

        method = handler_call_details.method
        metadata = handler_call_details.invocation_metadata
        signature = grpc_metadata_get(metadata, "sentry-signature")
        original = handler.unary_unary

        def request_deserializer(serialized_request: bytes) -> Any:
            _RPC_SIGNATURE_AUTH_TLS.failed = False

            if not verify_rpc_request_signature_hmac(
                self._secrets, method, serialized_request, signature
            ):
                _RPC_SIGNATURE_AUTH_TLS.failed = True
                return inner_deserializer(b"")

            return inner_deserializer(serialized_request)

        def unary_unary(request: Any, context: grpc.ServicerContext) -> Any:
            if getattr(_RPC_SIGNATURE_AUTH_TLS, "failed", False):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Authentication failed")

            return original(request, context)

        return grpc.unary_unary_rpc_method_handler(
            unary_unary,
            request_deserializer=request_deserializer,
            response_serializer=handler.response_serializer,
        )


class HostTemporarilyUnavailable(Exception):
    """Raised when a host is temporarily unavailable and should be retried later."""

    pass


@dataclass
class HealthCheckSettings:
    file_path: Path
    touch_interval_sec: float


class TaskbrokerClient:
    """
    Taskworker RPC client wrapper

    When num_brokers is provided, the client will connect to all brokers
    and choose a new broker to pair with randomly every max_tasks_before_rebalance tasks.
    """

    def __init__(
        self,
        hosts: list[str],
        application: str,
        metrics: MetricsBackend,
        max_tasks_before_rebalance: int = DEFAULT_REBALANCE_AFTER,
        max_consecutive_unavailable_errors: int = DEFAULT_CONSECUTIVE_UNAVAILABLE_ERRORS,
        temporary_unavailable_host_timeout: int = DEFAULT_TEMPORARY_UNAVAILABLE_HOST_TIMEOUT,
        health_check_settings: HealthCheckSettings | None = None,
        rpc_secret: str | None = None,
        grpc_config: str | None = None,
    ) -> None:
        assert len(hosts) > 0, "You must provide at least one RPC host to connect to"
        self._application = application
        self._hosts = hosts
        self._rpc_secret = rpc_secret
        self._metrics = metrics

        self._grpc_options: list[tuple[str, Any]] = [
            ("grpc.max_receive_message_length", MAX_ACTIVATION_SIZE)
        ]
        if grpc_config:
            self._grpc_options.append(("grpc.service_config", grpc_config))

        logger.info(
            "taskworker.client.start", extra={"hosts": hosts, "options": self._grpc_options}
        )

        self._cur_host = random.choice(self._hosts)
        self._host_to_stubs_lock = threading.Lock()
        self._host_to_stubs: dict[str, ConsumerServiceStub] = {
            self._cur_host: self._connect_to_host(self._cur_host)
        }

        self._max_tasks_before_rebalance = max_tasks_before_rebalance
        self._num_tasks_before_rebalance = max_tasks_before_rebalance

        self._max_consecutive_unavailable_errors = max_consecutive_unavailable_errors
        self._num_consecutive_unavailable_errors = 0

        self._temporary_unavailable_hosts: dict[str, float] = {}
        self._temporary_unavailable_host_timeout = temporary_unavailable_host_timeout

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
        logger.info("taskworker.client.connect", extra={"host": host})
        channel = grpc.insecure_channel(host, options=self._grpc_options)
        secrets = parse_rpc_secret_list(self._rpc_secret)
        if secrets:
            channel = grpc.intercept_channel(channel, RequestSignatureInterceptor(secrets))
        return ConsumerServiceStub(channel)

    def _get_stub(self, host: str) -> ConsumerServiceStub:
        with self._host_to_stubs_lock:
            if host not in self._host_to_stubs:
                self._host_to_stubs[host] = self._connect_to_host(host)
            return self._host_to_stubs[host]

    def _check_consecutive_unavailable_errors(self) -> None:
        if self._num_consecutive_unavailable_errors >= self._max_consecutive_unavailable_errors:
            self._temporary_unavailable_hosts[self._cur_host] = (
                time.time() + self._temporary_unavailable_host_timeout
            )

    def _clear_temporary_unavailable_hosts(self) -> None:
        hosts_to_remove = []
        for host, timeout in self._temporary_unavailable_hosts.items():
            if time.time() >= timeout:
                hosts_to_remove.append(host)

        for host in hosts_to_remove:
            self._temporary_unavailable_hosts.pop(host)

    def _get_cur_stub(self) -> tuple[str, ConsumerServiceStub]:
        self._clear_temporary_unavailable_hosts()
        available_hosts = [h for h in self._hosts if h not in self._temporary_unavailable_hosts]
        if not available_hosts:
            # If all hosts are temporarily unavailable, wait for the shortest timeout
            current_time = time.time()
            shortest_timeout = min(self._temporary_unavailable_hosts.values())
            logger.info(
                "taskworker.client.no_available_hosts",
                extra={"sleeping for": shortest_timeout - current_time},
            )
            time.sleep(shortest_timeout - current_time)
            return self._get_cur_stub()  # try again

        if self._cur_host in self._temporary_unavailable_hosts:
            self._cur_host = random.choice(available_hosts)
            self._num_tasks_before_rebalance = self._max_tasks_before_rebalance
            self._num_consecutive_unavailable_errors = 0
            self._metrics.incr(
                "taskworker.client.loadbalancer.rebalance",
                tags={"reason": "unavailable_count_reached"},
            )
        elif self._num_tasks_before_rebalance == 0:
            self._cur_host = random.choice(available_hosts)
            self._num_tasks_before_rebalance = self._max_tasks_before_rebalance
            self._num_consecutive_unavailable_errors = 0
            self._metrics.incr(
                "taskworker.client.loadbalancer.rebalance",
                tags={"reason": "max_tasks_reached"},
            )

        stub = self._get_stub(self._cur_host)

        self._num_tasks_before_rebalance -= 1
        return self._cur_host, stub

    def get_task(self, namespace: str | None = None) -> InflightTaskActivation | None:
        """
        Fetch a pending task.

        If a namespace is provided, only tasks for that namespace will be fetched.
        This will return None if there are no tasks to fetch.
        """
        self._emit_health_check()

        request = GetTaskRequest(application=self._application, namespace=namespace)
        try:
            host, stub = self._get_cur_stub()
            with self._metrics.timer("taskworker.get_task.rpc", tags={"host": host}):
                response = stub.GetTask(request)
        except grpc.RpcError as err:
            self._metrics.incr(
                "taskworker.client.rpc_error", tags={"method": "GetTask", "status": err.code().name}
            )
            if err.code() == grpc.StatusCode.NOT_FOUND:
                # Because our current broker doesn't have any tasks, try rebalancing.
                self._num_tasks_before_rebalance = 0
                return None
            if err.code() == grpc.StatusCode.UNAVAILABLE:
                self._num_consecutive_unavailable_errors += 1
                self._check_consecutive_unavailable_errors()
            raise
        self._num_consecutive_unavailable_errors = 0
        self._temporary_unavailable_hosts.pop(host, None)
        if response.HasField("task"):
            self._metrics.incr(
                "taskworker.client.get_task",
                tags={"namespace": response.task.namespace},
            )
            return InflightTaskActivation(
                activation=response.task, host=host, receive_timestamp=time.monotonic()
            )
        return None

    def update_task(
        self,
        processing_result: ProcessingResult,
        fetch_next_task: FetchNextTask | None = None,
    ) -> InflightTaskActivation | None:
        """
        Update the status for a given task activation.

        The return value is the next task that should be executed.
        """
        self._emit_health_check()
        if fetch_next_task is not None:
            fetch_next_task.application = self._application

        self._metrics.incr(
            "taskworker.client.fetch_next", tags={"next": fetch_next_task is not None}
        )
        self._clear_temporary_unavailable_hosts()
        request = SetTaskStatusRequest(
            id=processing_result.task_id,
            status=processing_result.status,
            fetch_next_task=fetch_next_task,
        )

        try:
            if processing_result.host in self._temporary_unavailable_hosts:
                self._metrics.incr(
                    "taskworker.client.skipping_set_task_due_to_unavailable_host",
                    tags={"broker_host": processing_result.host},
                )
                raise HostTemporarilyUnavailable(
                    f"Host: {processing_result.host} is temporarily unavailable"
                )

            stub = self._get_stub(processing_result.host)
            with self._metrics.timer(
                "taskworker.update_task.rpc", tags={"host": processing_result.host}
            ):
                response = stub.SetTaskStatus(request)
        except grpc.RpcError as err:
            self._metrics.incr(
                "taskworker.client.rpc_error",
                tags={"method": "SetTaskStatus", "status": err.code().name},
            )
            if err.code() == grpc.StatusCode.NOT_FOUND:
                # The current broker is empty, switch.
                self._num_tasks_before_rebalance = 0

                return None
            if err.code() == grpc.StatusCode.UNAVAILABLE:
                self._num_consecutive_unavailable_errors += 1
                self._check_consecutive_unavailable_errors()
            raise

        self._num_consecutive_unavailable_errors = 0
        self._temporary_unavailable_hosts.pop(processing_result.host, None)
        if response.HasField("task"):
            return InflightTaskActivation(
                activation=response.task,
                host=processing_result.host,
                receive_timestamp=time.monotonic(),
            )
        return None


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
        max_tasks_before_rebalance: int = DEFAULT_REBALANCE_AFTER,
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

    def update_task(
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
